//! Lock-free MPSC queue — Vyukov's algorithm with single-consumer specialization.
//!
//! Two enums:
//! - [Item] (`pub(super)`): what producers push, what consumers pop.
//!   Variants: `Id(NodeId)`, `DeadlinePending`. Exhaustive dispatch
//!   site — no `unreachable!` needed by consumers.
//! - [NodeValue] (private): adds the `Initial` head-sentinel variant.
//!   `Initial` appears only as the queue's initial dummy head and is
//!   freed on the first pop; a sighting at the dequeue position is
//!   memory corruption (the only `unreachable!` in the file).
//!
//! # Push correctness
//!
//! Vyukov's MPSC push has a two-step commit: (1) atomic swap on tail,
//! (2) store to prev.next. Between these steps the list is
//! disconnected — the consumer's `try_pop_value` sees a null `next`
//! even though an item is logically enqueued.
//!
//! Two invariants make this safe without spin-waiting:
//!
//! - [ThreadLocalProducer](super::ThreadLocalProducer) is `!Send`,
//!   shares the thread with the consumer — no transient state is
//!   observable across the consumer's only chance to look.
//! - [Producer](super::Producer) always wakes the consumer's
//!   [Parker](crate::wake::Parker) after push. Even if the consumer
//!   sees false-empty and parks, the wake releases it; on retry the
//!   push is complete and visible. An earlier producer's half-link
//!   may still hide it, but that producer wakes after linking too.
//!
//! Breaking either (cross-thread push without a wake) loses items.

use crate::poll::marker::NodeId;

use alloc::boxed::Box;
use core::ptr;
use core::sync::atomic::{AtomicPtr, Ordering};

/// What producers push and consumers pop. Exhaustive over the events
/// the consumer dispatches on.
#[derive(Clone, Copy)]
pub(super) enum Item {
    Id(NodeId),
    DeadlinePending,
}

/// Node payload. `Initial` is the queue's dummy head; producers can
/// only construct `Item(_)`.
#[derive(Clone, Copy)]
enum NodeValue {
    Initial,
    Item(Item),
}

struct Node {
    value: NodeValue,
    next: AtomicPtr<Node>,
}

impl Node {
    fn initial() -> *mut Node {
        Self::with(NodeValue::Initial)
    }

    fn item(item: Item) -> *mut Node {
        Self::with(NodeValue::Item(item))
    }

    fn with(value: NodeValue) -> *mut Node {
        Box::into_raw(Box::new(Node {
            value,
            next: AtomicPtr::new(ptr::null_mut()),
        }))
    }
}

/// 64-byte alignment to place `head` and `tail` on distinct cache
/// lines. Producers hammer `tail`; the single consumer reads `head`.
/// Without padding both share a line and every producer push
/// invalidates the consumer's cached `head`. 64 covers x86_64 and
/// AArch64; ESP32-S3 (Xtensa) uses 32 — padding is still correct,
/// just slightly larger than needed.
#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> core::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

pub(super) struct VyukovQueue {
    head: CachePadded<AtomicPtr<Node>>,
    tail: CachePadded<AtomicPtr<Node>>,
}

impl VyukovQueue {
    pub(super) fn new() -> Self {
        let initial = Node::initial();
        Self {
            head: CachePadded(AtomicPtr::new(initial)),
            tail: CachePadded(AtomicPtr::new(initial)),
        }
    }

    /// Convenience for the most common push: a node-id wake.
    pub(super) fn push(&self, id: NodeId) {
        self.push_value(Item::Id(id));
    }

    /// Lock-free enqueue. Safe from any thread.
    ///
    /// Ownership: `tail.swap` atomically claims the tail position and
    /// gives us exclusive ownership of `prev` — no other producer can
    /// reach it after the swap. That exclusivity is what makes the
    /// next-store safe without a lock.
    pub(super) fn push_value(&self, item: Item) {
        let node = Node::item(item);
        let prev = self.tail.swap(node, Ordering::AcqRel);
        // Link old tail → new node. Consumer traverses head → … → tail
        // via next pointers, so this completes the insertion.
        // Safety: prev is valid and exclusively ours after the swap.
        unsafe { &*prev }.next.store(node, Ordering::Release);
    }

    /// Non-blocking dequeue. Single consumer only.
    ///
    /// Ownership: single consumer means we have exclusive access to
    /// `head` without any atomic swap — no other thread reads or
    /// writes it, so ownership is free by design (hence Relaxed
    /// ordering on the head load/store).
    pub(super) fn try_pop_value(&self) -> Option<Item> {
        // Relaxed: exclusive consumer — no contention on head.
        let head = self.head.load(Ordering::Relaxed);
        // Acquire: synchronizes with the producer's Release store on
        // prev.next, making the pushed value visible to us.
        // Safety: head is always valid (initial dummy or prior
        // consumed node acting as the new dummy).
        let next = unsafe { &*head }.next.load(Ordering::Acquire);

        // Empty: head's own value is the dummy — we never read it,
        // only advance past it.
        if next.is_null() {
            return None;
        }
        // Safety: next was pushed by a producer; valid until we free it.
        let value = unsafe { &*next }.value;
        let item = match value {
            NodeValue::Item(item) => item,
            NodeValue::Initial => {
                // Initial only exists as the queue's first head, freed
                // on the first pop and never re-enters. Sighting it at
                // the next position means memory corruption.
                unreachable!("Initial in dequeue position — memory corruption")
            }
        };
        // Relaxed: only the consumer writes head.
        self.head.store(next, Ordering::Relaxed);
        // Safety: old head is no longer referenced by anyone.
        unsafe { drop(Box::from_raw(head)) };
        Some(item)
    }
}

impl Drop for VyukovQueue {
    fn drop(&mut self) {
        // AtomicPtr is just a raw pointer — Rust doesn't know about
        // the heap allocations behind it, so there's no automatic
        // drop. We must manually free all nodes.

        // Drain remaining items — each try_pop_value frees the old
        // head. Both Item variants drain cleanly.
        while self.try_pop_value().is_some() {}

        // Free the final head (the initial dummy if the queue was
        // never used, or the last consumed node acting as the new
        // dummy). Always non-null — try_pop_value never sets head to
        // null.
        let head = self.head.load(Ordering::Relaxed);
        debug_assert!(!head.is_null(), "head should never be null");
        unsafe { drop(Box::from_raw(head)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn assert_id(item: Option<Item>, expected: NodeId) {
        match item {
            Some(Item::Id(id)) => assert_eq!(id, expected),
            _ => panic!("expected Id({expected})"),
        }
    }

    fn assert_deadline(item: Option<Item>) {
        match item {
            Some(Item::DeadlinePending) => {}
            _ => panic!("expected DeadlinePending"),
        }
    }

    #[test]
    fn empty_queue_returns_none() {
        let q = VyukovQueue::new();
        assert!(q.try_pop_value().is_none());
    }

    #[test]
    fn single_push_pop() {
        let q = VyukovQueue::new();
        q.push(42);
        assert_id(q.try_pop_value(), 42);
        assert!(q.try_pop_value().is_none());
    }

    #[test]
    fn fifo_ordering() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push(2);
        q.push(3);
        assert_id(q.try_pop_value(), 1);
        assert_id(q.try_pop_value(), 2);
        assert_id(q.try_pop_value(), 3);
        assert!(q.try_pop_value().is_none());
    }

    #[test]
    fn interleaved_push_pop() {
        let q = VyukovQueue::new();
        q.push(1);
        assert_id(q.try_pop_value(), 1);
        q.push(2);
        q.push(3);
        assert_id(q.try_pop_value(), 2);
        q.push(4);
        assert_id(q.try_pop_value(), 3);
        assert_id(q.try_pop_value(), 4);
        assert!(q.try_pop_value().is_none());
    }

    #[test]
    fn drop_with_items_remaining() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push(2);
        q.push(3);
        drop(q);
    }

    #[test]
    fn drop_empty_queue() {
        let q = VyukovQueue::new();
        drop(q);
    }

    #[test]
    fn drop_after_full_drain() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push(2);
        assert_id(q.try_pop_value(), 1);
        assert_id(q.try_pop_value(), 2);
        assert!(q.try_pop_value().is_none());
        drop(q);
    }

    #[test]
    fn cross_thread_push() {
        let q = Arc::new(VyukovQueue::new());
        let q2 = q.clone();
        thread::spawn(move || q2.push(99)).join().unwrap();
        assert_id(q.try_pop_value(), 99);
    }

    #[test]
    fn multiple_cross_thread_producers() {
        let q = Arc::new(VyukovQueue::new());
        let mut handles = Vec::new();
        for i in 0..10 {
            let q = q.clone();
            handles.push(thread::spawn(move || q.push(i)));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let mut results = Vec::new();
        while let Some(Item::Id(id)) = q.try_pop_value() {
            results.push(id);
        }
        results.sort();
        assert_eq!(results, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn node_id_zero_works() {
        let q = VyukovQueue::new();
        q.push(0);
        assert_id(q.try_pop_value(), 0);
    }

    #[test]
    fn push_value_deadline_pending() {
        let q = VyukovQueue::new();
        q.push_value(Item::DeadlinePending);
        assert_deadline(q.try_pop_value());
        assert!(q.try_pop_value().is_none());
    }

    #[test]
    fn fifo_across_variants() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push_value(Item::DeadlinePending);
        q.push(2);
        assert_id(q.try_pop_value(), 1);
        assert_deadline(q.try_pop_value());
        assert_id(q.try_pop_value(), 2);
        assert!(q.try_pop_value().is_none());
    }

    #[test]
    fn drop_drains_mixed_variants() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push_value(Item::DeadlinePending);
        q.push(2);
        q.push_value(Item::DeadlinePending);
        drop(q);
    }
}
