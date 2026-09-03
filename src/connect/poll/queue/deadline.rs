//! Per-thread deadline heap.
//!
//! [DeadlineHeap] is a min-heap of `(deadline, slot, generation)`
//! entries, each backed by a slot in a slot table. [Self::register]
//! returns a [TimerKey] identifying one timer; many timers per node
//! coexist. [Self::cancel] releases a timer early.
//!
//! Pure data structure. Coordination with the poll queue (sentinel
//! push, re-arm, etc.) lives in the [Scheduler](super::scheduler)
//! above — this module knows nothing about wakers, queues, or threads.
//!
//! # Slots and lazy removal
//!
//! [BinaryHeap] cannot remove arbitrary entries in better than O(n).
//! Cancel and fire instead vacate the timer's slot (bumping its
//! generation); the heap entry stays behind and is discarded when it
//! bubbles to the top and [Self::peek] / [Self::pop] see the mismatch.
//! The generation is a pure reuse guard: a recycled slot never matches
//! entries (or [TimerKey]s) from its previous life.
//!
//! Dead entries linger until their deadline reaches the heap top, so
//! transient heap size ~ cancel rate × deadline horizon. Accepted:
//! compaction deferred until long-timeout churn hurts in practice.

use super::timers::{Generation, SlotId, TimerKey};
use crate::connect::poll::marker::NodeId;

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

/// One scheduled wake. Stored inside the heap as `Reverse<DeadlineEntry>`
/// so [BinaryHeap]'s default max-ordering becomes min-by-deadline.
#[derive(Clone, Copy)]
struct DeadlineEntry {
    deadline: Instant,
    slot: SlotId,
    /// Snapshot of the slot's generation at register time. Compared
    /// against the live value on peek/pop to detect dead entries.
    generation: Generation,
}

impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Tie-break by slot then generation purely for determinism;
        // neither field carries semantic meaning at the same deadline.
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.slot.cmp(&other.slot))
            .then_with(|| self.generation.cmp(&other.generation))
    }
}

impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for DeadlineEntry {}

impl PartialEq for DeadlineEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

/// One timer slot. `node_id: None` = vacant.
struct Slot {
    /// Bumped on every vacate — a stale key or heap entry addressing
    /// this slot after reuse fails the generation match instead of
    /// cancelling/firing the new occupant.
    generation: Generation,
    // If this node is occupied. Can be vacant post a deadline and
    // before a new deadline is allocated in this slot.
    node_id: Option<NodeId>,
}

/// Per-thread min-heap of pending deadlines. See module docs.
pub struct DeadlineHeap {
    heap: BinaryHeap<Reverse<DeadlineEntry>>,
    /// Grows to the peak count of concurrently awaiting timers and
    /// stays there — vacated slots are recycled, never freed. Design
    /// choice: the high-water mark is small (~24 bytes/slot) and
    /// bounded by tasks, not churn, so shrinking isn't worth the code.
    slots: Vec<Slot>,
    /// Vacant slot indices, reused before growing `slots`. Same
    /// high-water bound as `slots`.
    free: Vec<SlotId>,
}

impl DeadlineHeap {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            slots: Vec::new(),
            free: Vec::new(),
        }
    }

    /// Register a wake request for `node_id` at `deadline`. Independent
    /// of any other timer — including others for the same node. The
    /// returned key cancels it; dropping the key just means the timer
    /// fires and is reclaimed then.
    pub fn register(&mut self, node_id: NodeId, deadline: Instant) -> TimerKey {
        // Recycle before growing: `slots` stays bounded by peak
        // concurrent timers, not by churn.
        let slot = match self.free.pop() {
            Some(slot) => {
                self.slots[slot.index()].node_id = Some(node_id);
                slot
            }
            None => {
                // len before push == index after push.
                let slot = SlotId(self.slots.len());
                self.slots.push(Slot {
                    generation: Generation::first(),
                    node_id: Some(node_id),
                });
                slot
            }
        };
        // Snapshot the occupancy's generation into both the heap entry
        // and the key — they address this timer only, never a later
        // occupant of the same slot.
        let generation = self.slots[slot.index()].generation;
        self.heap.push(Reverse(DeadlineEntry {
            deadline,
            slot,
            generation,
        }));
        TimerKey { slot, generation }
    }

    /// Release a timer before it fires. No-op if the key is dead
    /// (already fired or already cancelled). The heap entry dies
    /// lazily when it bubbles up. Key possession is authorization —
    /// a key names exactly one timer, whichever waker carries it.
    pub fn cancel(&mut self, key: TimerKey) {
        // Result dropped: a dead key (fired, cancelled, recycled) is a
        // legal no-op, guarded by the generation match inside.
        self.take_live(key.slot, key.generation);
    }

    /// Returns the earliest live deadline. Drains dead entries off the
    /// top of the heap as a side effect. `None` if no live entries
    /// remain.
    pub fn peek(&mut self) -> Option<Instant> {
        while let Some(&Reverse(top)) = self.heap.peek() {
            if self.is_live(top.slot, top.generation) {
                return Some(top.deadline);
            }
            // Dead — discard. peek just returned Some so pop is also
            // Some; we discard the return value, no unwrap needed.
            self.heap.pop();
        }
        None
    }

    /// Pop one live entry whose deadline is `<= now`, vacating its
    /// slot. `None` once no such entry exists (heap drained, or
    /// earliest live is in the future). Caller drains by looping
    /// until `None`.
    pub fn pop(&mut self, now: Instant) -> Option<NodeId> {
        while let Some(&Reverse(top)) = self.heap.peek() {
            if !self.is_live(top.slot, top.generation) {
                // Dead — discard and keep looking.
                self.heap.pop();
                continue;
            }
            if top.deadline > now {
                return None;
            }
            // Live and expired — physically pop and fire. peek just
            // returned Some so pop is also Some; `?` avoids unwrap.
            let Reverse(entry) = self.heap.pop()?;
            return self.take_live(entry.slot, entry.generation);
        }
        None
    }

    /// True iff `(slot, generation)` addresses an occupied slot of the
    /// same generation.
    fn is_live(&self, slot: SlotId, generation: Generation) -> bool {
        match self.slots.get(slot.index()) {
            Some(slot) => slot.generation == generation && slot.node_id.is_some(),
            None => false,
        }
    }

    /// Release a live slot: bump generation (invalidating lingering
    /// heap entries and [TimerKey]s), mark vacant, recycle. Returns
    /// the occupant; `None` if `(slot, generation)` was dead.
    fn take_live(&mut self, slot_id: SlotId, generation: Generation) -> Option<NodeId> {
        if !self.is_live(slot_id, generation) {
            return None;
        }
        let slot = &mut self.slots[slot_id.index()];
        let node_id = slot.node_id.take();
        // Bump at release, not at register: one step kills the spent
        // key AND its lingering heap entry before the slot recycles.
        slot.generation = slot.generation.next();
        self.free.push(slot_id);
        node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(offset_ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(offset_ms)
    }

    fn past(offset_ms: u64) -> Instant {
        Instant::now() - Duration::from_millis(offset_ms)
    }

    // ---- empty / basic ----

    #[test]
    fn empty_heap_has_no_deadline() {
        let mut heap = DeadlineHeap::new();
        assert_eq!(heap.peek(), None);
        assert_eq!(heap.pop(Instant::now()), None);
    }

    #[test]
    fn register_single_entry_appears_at_top() {
        let mut heap = DeadlineHeap::new();
        let deadline = at(100);
        heap.register(7, deadline);
        assert_eq!(heap.peek(), Some(deadline));
    }

    // ---- min-heap ordering ----

    #[test]
    fn peek_returns_earliest_deadline() {
        let mut heap = DeadlineHeap::new();
        let early = at(10);
        heap.register(0, at(100));
        heap.register(1, early);
        heap.register(2, at(50));
        assert_eq!(heap.peek(), Some(early));
    }

    #[test]
    fn pop_expired_drains_in_deadline_order() {
        let mut heap = DeadlineHeap::new();
        heap.register(2, past(10));
        heap.register(0, past(30));
        heap.register(1, past(20));
        let now = Instant::now();
        assert_eq!(heap.pop(now), Some(0));
        assert_eq!(heap.pop(now), Some(1));
        assert_eq!(heap.pop(now), Some(2));
        assert_eq!(heap.pop(now), None);
        assert_eq!(heap.peek(), None);
    }

    #[test]
    fn pop_expired_leaves_future_entries() {
        let mut heap = DeadlineHeap::new();
        let future = at(60_000);
        heap.register(0, past(10));
        heap.register(1, future);
        let now = Instant::now();
        assert_eq!(heap.pop(now), Some(0));
        assert_eq!(heap.pop(now), None);
        assert_eq!(heap.peek(), Some(future));
    }

    // ---- many timers per node ----

    #[test]
    fn same_node_holds_independent_timers() {
        let mut heap = DeadlineHeap::new();
        let early = at(10);
        heap.register(5, at(60_000));
        heap.register(5, early);
        heap.register(5, at(30_000));
        // All three live; earliest bounds the park.
        assert_eq!(heap.peek(), Some(early));
    }

    #[test]
    fn same_node_expired_timers_all_fire() {
        let mut heap = DeadlineHeap::new();
        heap.register(5, past(30));
        heap.register(5, past(20));
        heap.register(5, past(10));
        let now = Instant::now();
        assert_eq!(heap.pop(now), Some(5));
        assert_eq!(heap.pop(now), Some(5));
        assert_eq!(heap.pop(now), Some(5));
        assert_eq!(heap.pop(now), None);
    }

    // ---- cancellation ----

    #[test]
    fn cancelled_timer_never_fires() {
        let mut heap = DeadlineHeap::new();
        let key = heap.register(0, past(10));
        heap.cancel(key);
        assert_eq!(heap.peek(), None);
        assert_eq!(heap.pop(Instant::now()), None);
    }

    #[test]
    fn cancel_leaves_other_timers_live() {
        let mut heap = DeadlineHeap::new();
        let keep = at(50_000);
        let key = heap.register(0, at(10));
        heap.register(1, keep);
        heap.cancel(key);
        // Dead earliest entry is drained; live one surfaces.
        assert_eq!(heap.peek(), Some(keep));
    }

    #[test]
    fn cancel_is_idempotent() {
        let mut heap = DeadlineHeap::new();
        let key = heap.register(0, at(10));
        heap.cancel(key);
        heap.cancel(key);
        assert_eq!(heap.peek(), None);
    }

    #[test]
    fn cancel_after_fire_is_noop() {
        let mut heap = DeadlineHeap::new();
        let key = heap.register(0, past(10));
        assert_eq!(heap.pop(Instant::now()), Some(0));
        heap.cancel(key);
        assert_eq!(heap.peek(), None);
    }

    // ---- slot reuse / ABA ----

    #[test]
    fn recycled_slot_ignores_stale_key_and_entry() {
        let mut heap = DeadlineHeap::new();
        let stale = heap.register(0, past(10));
        heap.cancel(stale);
        // Reuses the slot with a bumped generation.
        let fresh = heap.register(1, at(50_000));
        assert_eq!(stale.slot, fresh.slot);
        assert_ne!(stale.generation, fresh.generation);
        // Stale key must not kill the new occupant.
        heap.cancel(stale);
        assert!(heap.peek().is_some());
        // Stale heap entry (expired!) must not fire as the new node.
        assert_eq!(heap.pop(Instant::now()), None);
    }

    #[test]
    fn fired_slot_is_reused() {
        let mut heap = DeadlineHeap::new();
        let first = heap.register(0, past(10));
        assert_eq!(heap.pop(Instant::now()), Some(0));
        let second = heap.register(1, past(10));
        assert_eq!(second.slot, first.slot);
        assert_eq!(heap.pop(Instant::now()), Some(1));
    }

    // ---- dead entries ----

    #[test]
    fn heap_of_corpses_peeks_none() {
        let mut heap = DeadlineHeap::new();
        let a = heap.register(0, at(10_000));
        let b = heap.register(1, at(20_000));
        heap.cancel(a);
        heap.cancel(b);
        // Heap still physically holds both entries; peek sees through.
        assert_eq!(heap.peek(), None);
    }
}
