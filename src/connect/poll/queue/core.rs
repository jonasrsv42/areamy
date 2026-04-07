//! Lock-free MPSC queue — Vyukov's algorithm with single-consumer specialization.
//!
//! Private to the queue module. Accessed through typed handles:
//! [Consumer](super::Consumer), [Producer](super::Producer),
//! [ThreadLocalProducer](super::ThreadLocalProducer).
//!
//! # Transient inconsistency and correctness
//!
//! Vyukov's MPSC push has a two-step commit: (1) atomic swap on tail,
//! (2) store to prev.next. Between these steps the list is disconnected —
//! the consumer's `try_pop` sees a null `next` even though an item is
//! logically enqueued.
//!
//! In the general case this requires a spin-wait in the consumer. Our
//! implementation avoids this by relying on two invariants enforced by
//! the typed handle system:
//!
//! - **[ThreadLocalProducer](super::ThreadLocalProducer)** is `!Send` —
//!   it runs on the same thread as the [Consumer](super::Consumer).
//!   Both steps of push complete before the consumer can call `try_pop`.
//!   No transient state is observable.
//!
//! - **[Producer](super::Producer)** is `Send + Sync` and always calls
//!   `signal()` after `push()`. Even if the consumer sees a false empty
//!   and blocks on the condvar, the producer's signal wakes it. On retry
//!   both steps have completed and the item is visible.
//!
//! **If these invariants are broken (e.g. cross-thread push without signal),
//! the consumer can miss items and deadlock.** The typed handles prevent
//! this at compile time.

use crate::connect::poll::marker::NodeId;
use crate::error::Error;
use crate::fatal;

use alloc::boxed::Box;
use core::ptr;
use core::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Condvar, Mutex};

enum Value {
    Sentinel,
    Id(NodeId),
}

struct Node {
    value: Value,
    next: AtomicPtr<Node>,
}

impl Node {
    fn sentinel() -> *mut Node {
        Box::into_raw(Box::new(Node {
            value: Value::Sentinel,
            next: AtomicPtr::new(ptr::null_mut()),
        }))
    }

    fn heap(id: NodeId) -> *mut Node {
        Box::into_raw(Box::new(Node {
            value: Value::Id(id),
            next: AtomicPtr::new(ptr::null_mut()),
        }))
    }
}

/// Lock-free MPSC queue. Producers swap onto tail, consumer reads from head.
pub(super) struct VyukovQueue {
    head: AtomicPtr<Node>,
    tail: AtomicPtr<Node>,
    parked: Mutex<bool>,
    signal: Condvar,
}

impl VyukovQueue {
    pub(super) fn new() -> Self {
        let sentinel = Node::sentinel();
        Self {
            head: AtomicPtr::new(sentinel),
            tail: AtomicPtr::new(sentinel),
            parked: Mutex::new(false),
            signal: Condvar::new(),
        }
    }

    /// Lock-free enqueue. Safe from any thread.
    ///
    /// Ownership: `tail.swap` gives exclusive ownership of the previous
    /// tail node — no other producer can reach it after the swap. This
    /// is what makes it safe to store into `prev.next` without a lock.
    pub(super) fn push(&self, id: NodeId) {
        let node = Node::heap(id);
        // Atomically claim the tail position — gives us exclusive
        // ownership of prev (the old tail)
        let prev = self.tail.swap(node, Ordering::AcqRel);
        // Link old tail → new node. Consumer traverses head → ... → tail
        // via next pointers, so this completes the insertion.
        // Safety: prev is valid and exclusively ours after the swap
        unsafe { &*prev }.next.store(node, Ordering::Release);
    }

    /// Signal the consumer that items are available.
    pub(super) fn signal(&self) -> Result<(), Error> {
        let mut parked = self.parked.lock().map_err(|e| fatal!(e))?;
        *parked = true;
        self.signal.notify_one();
        drop(parked);
        Ok(())
    }

    /// Non-blocking dequeue. Single consumer only.
    ///
    /// Ownership: single consumer means we have exclusive access to
    /// head without any atomic swap — no other thread reads or writes
    /// head, so ownership is free by design.
    pub(super) fn try_pop(&self) -> Option<NodeId> {
        // Relaxed: exclusive consumer — no contention on head
        let head = self.head.load(Ordering::Relaxed);
        // Acquire: synchronizes with producer's Release store on prev.next
        // Safety: head is always valid (sentinel or previously consumed node)
        let next = unsafe { &*head }.next.load(Ordering::Acquire);

        // Sentinel (or empty queue): next is null means no items after head.
        // Head acts as the sentinel — we never read its value, only its next.
        if next.is_null() {
            return None;
        }
        // next is a node pushed by a producer. Producers only create
        // Value::Id nodes — Value::Sentinel only exists in the initial
        // sentinel created in new(), which is freed on the first pop and
        // never re-enters the list.
        let id = match unsafe { &*next }.value {
            Value::Id(id) => id,
            Value::Sentinel => {
                unreachable!("sentinel in dequeue position — indicates memory corruption")
            }
        };
        // Relaxed: only consumer writes head
        self.head.store(next, Ordering::Relaxed);
        // Safety: old head is no longer referenced by anyone
        unsafe { drop(Box::from_raw(head)) };
        Some(id)
    }

    /// Blocking dequeue. Single consumer only.
    ///
    /// # Same-thread assumption
    ///
    /// [ThreadLocalProducer](super::ThreadLocalProducer) calls `push`
    /// without `signal`. This is safe because `push` and `next` run on
    /// the same thread — if we're in `next`, no local push can happen
    /// concurrently. Local pushes only happen during poll callbacks,
    /// which only run when `next` has already returned.
    ///
    /// If a local push could happen while we're parked in `wait`, we
    /// would deadlock — no signal to wake us, item stuck in the queue.
    /// The `!Send` constraint on ThreadLocalProducer and Consumer
    /// guarantees they share a thread, preventing this.
    ///
    /// Only cross-thread [Producer](super::Producer) can wake us from
    /// park, and it always calls `signal`.
    pub(super) fn next(&self) -> Result<NodeId, Error> {
        loop {
            // Fast path: item already available, no blocking needed.
            // Handles local pushes that completed before we entered next.
            if let Some(id) = self.try_pop() {
                return Ok(id);
            }

            // Acquire park lock before re-checking — prevents race where
            // a cross-thread producer pushes + signals between our
            // try_pop and the wait.
            let mut parked = self.parked.lock().map_err(|e| fatal!(e))?;

            // Re-check under lock: a cross-thread producer may have
            // pushed between our first try_pop and acquiring the lock.
            if let Some(id) = self.try_pop() {
                return Ok(id);
            }

            // Queue is truly empty. Sleep until a cross-thread producer
            // calls signal. Local producers cannot run while we're
            // parked (same thread), so no local items can appear.
            while !*parked {
                parked = self.signal.wait(parked).map_err(|e| fatal!(e))?;
            }

            // A producer signaled. Reset flag and loop to try_pop.
            // The producer's push may still be in the transient state
            // (tail swapped but next not yet linked), but signal()
            // acquires the mutex AFTER completing the next store,
            // so by the time we wake the item is fully visible.
            *parked = false;
        }
    }
}

impl Drop for VyukovQueue {
    fn drop(&mut self) {
        // AtomicPtr is just a raw pointer — Rust doesn't know about the
        // heap allocations behind it, so there's no automatic drop.
        // We must manually free all nodes.

        // Drain remaining items — each try_pop frees the old head
        while self.try_pop().is_some() {}

        // Free the final head node. After draining, head points to the
        // last node acting as sentinel (the original sentinel if the queue
        // was never used, or the last consumed node otherwise). It is
        // always non-null — try_pop never sets head to null.
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

    #[test]
    fn empty_queue_returns_none() {
        let q = VyukovQueue::new();
        assert_eq!(q.try_pop(), None);
    }

    #[test]
    fn single_push_pop() {
        let q = VyukovQueue::new();
        q.push(42);
        assert_eq!(q.try_pop(), Some(42));
        // Sentinel freed on first pop, head is now the consumed node
        assert_eq!(q.try_pop(), None);
    }

    #[test]
    fn fifo_ordering() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push(2);
        q.push(3);
        assert_eq!(q.try_pop(), Some(1));
        assert_eq!(q.try_pop(), Some(2));
        assert_eq!(q.try_pop(), Some(3));
        assert_eq!(q.try_pop(), None);
    }

    #[test]
    fn interleaved_push_pop() {
        let q = VyukovQueue::new();
        q.push(1);
        assert_eq!(q.try_pop(), Some(1));
        q.push(2);
        q.push(3);
        assert_eq!(q.try_pop(), Some(2));
        q.push(4);
        assert_eq!(q.try_pop(), Some(3));
        assert_eq!(q.try_pop(), Some(4));
        assert_eq!(q.try_pop(), None);
    }

    #[test]
    fn drop_with_items_remaining() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push(2);
        q.push(3);
        // Drop without popping — should free all nodes without leak
        drop(q);
    }

    #[test]
    fn drop_empty_queue() {
        let q = VyukovQueue::new();
        // Drop without any push — should free only the sentinel
        drop(q);
    }

    #[test]
    fn drop_after_full_drain() {
        let q = VyukovQueue::new();
        q.push(1);
        q.push(2);
        assert_eq!(q.try_pop(), Some(1));
        assert_eq!(q.try_pop(), Some(2));
        assert_eq!(q.try_pop(), None);
        // Drop after draining — should free the last consumed node
        drop(q);
    }

    #[test]
    fn cross_thread_push() {
        let q = Arc::new(VyukovQueue::new());
        let q2 = q.clone();

        let handle = thread::spawn(move || {
            q2.push(99);
            q2.signal().unwrap();
        });

        q.next().unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn multiple_cross_thread_producers() {
        let q = Arc::new(VyukovQueue::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let q = q.clone();
            handles.push(thread::spawn(move || {
                q.push(i);
                q.signal().unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut results = Vec::new();
        while let Some(id) = q.try_pop() {
            results.push(id);
        }
        results.sort();
        assert_eq!(results, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn node_id_zero_works() {
        let q = VyukovQueue::new();
        q.push(0);
        assert_eq!(q.try_pop(), Some(0));
    }
}
