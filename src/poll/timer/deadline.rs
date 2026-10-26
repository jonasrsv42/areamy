//! Per-thread deadline heap.
//!
//! [DeadlineHeap] is a min-heap of `(deadline, cell, generation)`
//! entries, each backed by a cell in a cell table. [DeadlineHeap::register]
//! returns a [TimerKey] identifying one timer; many timers per node
//! coexist. [DeadlineHeap::cancel] releases a timer early.
//!
//! Pure data structure: it knows nothing about wakers, stacks or threads.
//!
//! # Cells and lazy removal
//!
//! [BinaryHeap] cannot remove arbitrary entries in better than O(n).
//! Cancel and fire instead vacate the timer's cell (bumping its
//! generation); the heap entry stays behind and is discarded when it
//! bubbles to the top and [DeadlineHeap::peek] / [DeadlineHeap::pop] see the mismatch.
//! The generation is a pure reuse guard: a recycled cell never matches
//! entries (or [TimerKey]s) from its previous life.
//!
//! Dead entries linger until their deadline reaches the heap top, so
//! transient heap size ~ cancel rate × deadline horizon. Accepted:
//! compaction deferred until long-timeout churn hurts in practice.

use super::TimerKey;
use super::key::{CellId, Generation};
use crate::poll::graph::NodeId;

use alloc::collections::BinaryHeap;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::cmp::Reverse;
use std::time::Instant;

/// One scheduled wake. Stored inside the heap as `Reverse<DeadlineEntry>`
/// so [BinaryHeap]'s default max-ordering becomes min-by-deadline.
#[derive(Clone, Copy)]
struct DeadlineEntry {
    deadline: Instant,
    cell: CellId,
    /// Snapshot of the cell's generation at register time. Compared
    /// against the live value on peek/pop to detect dead entries.
    generation: Generation,
}

impl Ord for DeadlineEntry {
    /// O(1): up to three compares.
    fn cmp(&self, other: &Self) -> Ordering {
        // Tie-break by cell then generation purely for determinism;
        // neither field carries semantic meaning at the same deadline.
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.cell.cmp(&other.cell))
            .then_with(|| self.generation.cmp(&other.generation))
    }
}

impl PartialOrd for DeadlineEntry {
    /// O(1): [Ord::cmp].
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for DeadlineEntry {}

impl PartialEq for DeadlineEntry {
    /// O(1): [Ord::cmp].
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

/// One timer cell. `node_id: None` = vacant.
struct Cell {
    /// Bumped on every vacate — a stale key or heap entry addressing
    /// this cell after reuse fails the generation match instead of
    /// cancelling/firing the new occupant.
    generation: Generation,
    // If this node is occupied. Can be vacant post a deadline and
    // before a new deadline is allocated in this cell.
    node_id: Option<NodeId>,
}

/// Per-thread min-heap of pending deadlines. See module docs.
/// Costs: h heap entries, dead ones included.
pub(crate) struct DeadlineHeap {
    heap: BinaryHeap<Reverse<DeadlineEntry>>,
    /// Grows to the peak count of concurrently awaiting timers and
    /// stays there — vacated cells are recycled, never freed. Design
    /// choice: the high-water mark is small (~24 bytes/cell) and
    /// bounded by tasks, not churn, so shrinking isn't worth the code.
    cells: Vec<Cell>,
    /// Vacant cell indices, reused before growing `cells`. Same
    /// high-water bound as `cells`.
    free: Vec<CellId>,
}

impl DeadlineHeap {
    /// O(1): no allocation.
    pub(crate) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            cells: Vec::new(),
            free: Vec::new(),
        }
    }

    /// Register a wake request for `node_id` at `deadline`. Independent
    /// of any other timer — including others for the same node. The
    /// returned key cancels it; dropping the key just means the timer
    /// fires and is reclaimed then.
    /// O(log h) worst case, O(1) for a deadline later than those queued: a recycled cell or a
    /// push, and a heap push sifting past the later deadlines; no RMW.
    pub(crate) fn register(&mut self, node_id: NodeId, deadline: Instant) -> TimerKey {
        // Recycle before growing: `cells` stays bounded by peak
        // concurrent timers, not by churn.
        let cell = match self.free.pop() {
            Some(cell) => {
                self.cells[cell.index()].node_id = Some(node_id);
                cell
            }
            None => {
                // len before push == index after push.
                let cell = CellId(self.cells.len());
                self.cells.push(Cell {
                    generation: Generation::first(),
                    node_id: Some(node_id),
                });
                cell
            }
        };
        // Snapshot the occupancy's generation into both the heap entry
        // and the key — they address this timer only, never a later
        // occupant of the same cell.
        let generation = self.cells[cell.index()].generation;
        self.heap.push(Reverse(DeadlineEntry {
            deadline,
            cell,
            generation,
        }));
        TimerKey { cell, generation }
    }

    /// Release a timer before it fires. No-op if the key is dead
    /// (already fired or already cancelled). The heap entry dies
    /// lazily when it bubbles up. Key possession is authorization —
    /// a key names exactly one timer, whichever waker carries it.
    /// O(1): a [Self::take_live]; [Self::peek] or [Self::pop] discards the entry later, in
    /// O(log h).
    pub(crate) fn cancel(&mut self, key: TimerKey) {
        // Result dropped: a dead key (fired, cancelled, recycled) is a
        // legal no-op, guarded by the generation match inside.
        self.take_live(key.cell, key.generation);
    }

    /// Returns the earliest live deadline. Drains dead entries off the
    /// top of the heap as a side effect. `None` if no live entries
    /// remain.
    /// O(d log h) for d dead entries on top: O(1) amortized, each discard charged to its cancel.
    /// A live top: a load and a [Self::is_live].
    pub(crate) fn peek(&mut self) -> Option<Instant> {
        while let Some(&Reverse(top)) = self.heap.peek() {
            if self.is_live(top.cell, top.generation) {
                return Some(top.deadline);
            }
            // Dead — discard. peek just returned Some so pop is also
            // Some; we discard the return value, no unwrap needed.
            self.heap.pop();
        }
        None
    }

    /// Pop one live entry whose deadline is `<= now`, vacating its
    /// cell. `None` once no such entry exists (heap drained, or
    /// earliest live is in the future). Caller drains by looping
    /// until `None`.
    /// O(log h) for the entry popped, a sift-down of about log h compares, plus dead ones drained
    /// as in [Self::peek]. None due: as [Self::peek], and a compare.
    pub(crate) fn pop(&mut self, now: Instant) -> Option<NodeId> {
        while let Some(&Reverse(top)) = self.heap.peek() {
            if !self.is_live(top.cell, top.generation) {
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
            return self.take_live(entry.cell, entry.generation);
        }
        None
    }

    /// True iff `(cell, generation)` addresses an occupied cell of the
    /// same generation. O(1): a bounds check, two loads and two compares.
    fn is_live(&self, cell: CellId, generation: Generation) -> bool {
        match self.cells.get(cell.index()) {
            Some(cell) => cell.generation == generation && cell.node_id.is_some(),
            None => false,
        }
    }

    /// Release a live cell: bump generation (invalidating lingering
    /// heap entries and [TimerKey]s), mark vacant, recycle. Returns
    /// the occupant; `None` if `(cell, generation)` was dead.
    /// O(1) amortized: an [Self::is_live], two stores and a free-list push.
    fn take_live(&mut self, cell_id: CellId, generation: Generation) -> Option<NodeId> {
        if !self.is_live(cell_id, generation) {
            return None;
        }
        let cell = &mut self.cells[cell_id.index()];
        let node_id = cell.node_id.take();
        // Bump at release, not at register: one step kills the spent
        // key AND its lingering heap entry before the cell recycles.
        cell.generation = cell.generation.next();
        self.free.push(cell_id);
        node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;

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

    #[test]
    fn a_deadline_of_exactly_now_fires() {
        let mut heap = DeadlineHeap::new();
        let now = Instant::now();
        heap.register(0, now);
        assert_eq!(heap.pop(now), Some(0));
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

    // ---- cell reuse / ABA ----

    #[test]
    fn recycled_cell_ignores_stale_key_and_entry() {
        let mut heap = DeadlineHeap::new();
        let stale = heap.register(0, past(10));
        heap.cancel(stale);
        // Reuses the cell with a bumped generation.
        let fresh = heap.register(1, at(50_000));
        assert_eq!(stale.cell, fresh.cell);
        assert_ne!(stale.generation, fresh.generation);
        // Stale key must not kill the new occupant.
        heap.cancel(stale);
        assert!(heap.peek().is_some());
        // Stale heap entry (expired!) must not fire as the new node.
        assert_eq!(heap.pop(Instant::now()), None);
    }

    #[test]
    fn fired_cell_is_reused() {
        let mut heap = DeadlineHeap::new();
        let first = heap.register(0, past(10));
        assert_eq!(heap.pop(Instant::now()), Some(0));
        let second = heap.register(1, past(10));
        assert_eq!(second.cell, first.cell);
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
