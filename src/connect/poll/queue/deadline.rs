//! Per-thread deadline heap.
//!
//! [DeadlineHeap] is a min-heap of `(deadline, node_id, generation)`
//! entries. It records "node N wants to be polled at instant T"
//! requests and surfaces the earliest such request via
//! [DeadlineHeap::peek], or pops expired requests via
//! [DeadlineHeap::pop].
//!
//! Pure data structure. Coordination with the poll queue (sentinel
//! push, re-arm, etc.) lives in the [Scheduler](super::scheduler)
//! above — this module knows nothing about wakers, queues, or threads.
//!
//! # Why a generation counter
//!
//! [BinaryHeap] cannot remove arbitrary entries in better than O(n).
//! When a node re-registers with a new deadline, we don't try to find
//! and remove its old entry — instead we bump the node's current
//! generation and tag the new entry with it. When an old entry
//! bubbles to the top, [DeadlineHeap::peek] / [DeadlineHeap::pop]
//! compare its tag against the live generation and discard stale
//! ones.
//!
//! Common case (one deadline per node, fired before any
//! re-registration) is O(log n) per op with no skipped entries.

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
    node_id: NodeId,
    /// Snapshot of `generations[node_id]` at register time. Compared
    /// against the live value on pop to detect stale entries.
    generation: u64,
}

impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Tie-break by node_id then generation purely for determinism;
        // neither field carries semantic meaning at the same deadline.
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.node_id.cmp(&other.node_id))
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

/// Per-thread min-heap of pending deadlines. See module docs.
pub struct DeadlineHeap {
    heap: BinaryHeap<Reverse<DeadlineEntry>>,
    /// Per-node current generation. Grows on demand in [Self::register]
    /// and [Self::invalidate]. An entry is fresh iff its generation
    /// equals `generations[node_id]`.
    generations: Vec<u64>,
}

impl DeadlineHeap {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            generations: Vec::new(),
        }
    }

    /// Register a wake request for `node_id` at `deadline`. Any prior
    /// request for the same node is superseded — the old entry remains
    /// in the heap but is now stale; it gets dropped when it bubbles up.
    pub fn register(&mut self, node_id: NodeId, deadline: Instant) {
        let generation = self.bump_generation(node_id);
        self.heap.push(Reverse(DeadlineEntry {
            deadline,
            node_id,
            generation,
        }));
    }

    /// Returns the earliest active deadline. Drains stale entries off
    /// the top of the heap as a side effect. `None` if no fresh
    /// entries remain.
    pub fn peek(&mut self) -> Option<Instant> {
        while let Some(&Reverse(top)) = self.heap.peek() {
            if self.is_fresh(&top) {
                return Some(top.deadline);
            }
            // Stale — discard. peek just returned Some so pop is also
            // Some; we discard the return value, no unwrap needed.
            self.heap.pop();
        }
        None
    }

    /// Pop one fresh entry whose deadline is `<= now`. `None` once no
    /// such entry exists (heap drained, or earliest fresh is in the
    /// future). Caller drains by looping until `None`.
    pub fn pop(&mut self, now: Instant) -> Option<NodeId> {
        while let Some(&Reverse(top)) = self.heap.peek() {
            if top.deadline > now {
                return None;
            }
            // Past deadline — physically pop. peek just returned Some
            // so pop is also Some; we destructure with let-else to
            // avoid unwrap. The unreachable branch is genuinely
            // unreachable: peek and pop see the same heap state under
            // &mut self.
            let Some(Reverse(entry)) = self.heap.pop() else {
                return None;
            };
            if self.is_fresh(&entry) {
                return Some(entry.node_id);
            }
            // Stale — continue draining.
        }
        None
    }

    /// True iff the heap holds no entries (including stale ones).
    /// Reflects raw count; stale entries are still counted until they
    /// reach the top and are drained by peek/pop.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn bump_generation(&mut self, node_id: NodeId) -> u64 {
        if node_id >= self.generations.len() {
            self.generations.resize(node_id + 1, 0);
        }
        // Wrap on overflow rather than panic. `is_fresh` does exact
        // match, so a wrap (after 2^64 re-registers — ~584 years at
        // 1 GHz) just means the new entry uses a recycled tag. Any
        // long-lingering entry from the previous cycle through that
        // tag would be falsely fresh — astronomically unlikely.
        self.generations[node_id] = self.generations[node_id].wrapping_add(1);
        self.generations[node_id]
    }

    fn is_fresh(&self, entry: &DeadlineEntry) -> bool {
        self.generations.get(entry.node_id).copied() == Some(entry.generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(offset_ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(offset_ms)
    }

    // ---- empty / basic ----

    #[test]
    fn empty_heap_has_no_deadline() {
        let mut heap = DeadlineHeap::new();
        assert_eq!(heap.peek(), None);
        assert_eq!(heap.pop(Instant::now()), None);
        assert!(heap.is_empty());
    }

    #[test]
    fn register_single_entry_appears_at_top() {
        let mut heap = DeadlineHeap::new();
        let deadline = at(100);
        heap.register(7, deadline);
        assert_eq!(heap.peek(), Some(deadline));
        assert!(!heap.is_empty());
    }

    #[test]
    fn register_grows_generations_on_demand() {
        let mut heap = DeadlineHeap::new();
        heap.register(42, at(10));
        assert_eq!(heap.generations.len(), 43);
    }

    // ---- min-heap ordering ----

    #[test]
    fn peek_returns_earliest_deadline() {
        let mut heap = DeadlineHeap::new();
        let early = at(10);
        let mid = at(50);
        let late = at(100);
        heap.register(0, late);
        heap.register(1, early);
        heap.register(2, mid);
        assert_eq!(heap.peek(), Some(early));
    }

    #[test]
    fn pop_expired_drains_in_deadline_order() {
        let mut heap = DeadlineHeap::new();
        let t0 = Instant::now() - Duration::from_millis(30);
        let t1 = Instant::now() - Duration::from_millis(20);
        let t2 = Instant::now() - Duration::from_millis(10);
        heap.register(2, t2);
        heap.register(0, t0);
        heap.register(1, t1);
        let now = Instant::now();
        assert_eq!(heap.pop(now), Some(0));
        assert_eq!(heap.pop(now), Some(1));
        assert_eq!(heap.pop(now), Some(2));
        assert_eq!(heap.pop(now), None);
    }

    #[test]
    fn pop_expired_leaves_future_entries() {
        let mut heap = DeadlineHeap::new();
        let past = Instant::now() - Duration::from_millis(10);
        let future = at(60_000);
        heap.register(0, past);
        heap.register(1, future);
        let now = Instant::now();
        assert_eq!(heap.pop(now), Some(0));
        assert_eq!(heap.pop(now), None);
        assert_eq!(heap.peek(), Some(future));
    }

    // ---- staleness via generations ----

    #[test]
    fn reregister_supersedes_prior_entry() {
        let mut heap = DeadlineHeap::new();
        let old = Instant::now() - Duration::from_millis(50);
        let new = at(60_000);
        heap.register(0, old);
        heap.register(0, new);
        // peek_deadline drains the stale (old, generation 1) entry
        // and surfaces the fresh one (generation 2).
        assert_eq!(heap.peek(), Some(new));
        // The stale-past entry must not surface as expired.
        assert_eq!(heap.pop(Instant::now()), None);
    }

    #[test]
    fn peek_drains_many_stale_entries_above_fresh_one() {
        let mut heap = DeadlineHeap::new();
        let earliest = Instant::now() - Duration::from_millis(100);
        // Same node, many re-registrations — only the last is fresh.
        for _ in 0..5 {
            heap.register(0, earliest);
        }
        let later = at(50_000);
        heap.register(1, later);
        let now = Instant::now();
        assert_eq!(heap.pop(now), Some(0));
        assert_eq!(heap.pop(now), None);
        assert_eq!(heap.peek(), Some(later));
    }
}
