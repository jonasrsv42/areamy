//! Lock-free MPSC poll queue with two-stage construction.
//!
//! Stage 1 (main thread): [PollQueue] creates [Producer]s for
//! cross-thread wakers. `PollQueue` is `Send` — crosses threads — but
//! not `Sync`: it owns the consumer's parker, which one thread parks on.
//!
//! Stage 2 (async thread): [PollQueue::local] consumes the config
//! and returns [Consumer] + [ThreadLocalProducer] for the async thread.
//!
//! - [Consumer]: blocking dequeue, `!Send`, `!Sync`
//! - [Producer]: enqueue + wake the consumer, `Send + Sync + Clone`
//! - [ThreadLocalProducer]: enqueue + schedule, `Clone`, `!Send`, `!Sync`

mod consumer;
mod core;
mod deadline;
mod producer;
mod scheduler;
mod thread_local;

pub use consumer::Consumer;
pub use producer::Producer;
pub use thread_local::ThreadLocalProducer;

use crate::wake::Parker;
use ::core::cell::RefCell;
use alloc::rc::Rc;
use alloc::sync::Arc;

/// ```compile_fail
/// use areamy::poll::queue::PollQueue;
/// fn require_sync<T: Sync>() {}
/// require_sync::<PollQueue>(); // must not compile: it owns the consumer's parker
/// ```
pub struct PollQueue {
    /// Shared by every producer and the consumer.
    inner: Arc<core::VyukovQueue>,
    /// Built here, before the poll thread exists: producers take its waker now, the consumer
    /// takes the parker itself in [Self::local].
    parker: Parker,
}

impl Default for PollQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl PollQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(core::VyukovQueue::new()),
            parker: Parker::new(),
        }
    }

    pub fn producer(&self) -> Producer {
        Producer {
            inner: self.inner.clone(),
            // Every producer wakes the one consumer.
            waker: self.parker.waker(),
        }
    }

    /// Consume config and create the async-thread handles.
    /// Must be called on the async thread — returns `!Send` types.
    pub fn local(self) -> (Consumer, ThreadLocalProducer) {
        // The parker moves to the thread that parks on it; producers keep only its waker.
        let scheduler = Rc::new(RefCell::new(scheduler::Scheduler::new(
            self.inner,
            self.parker,
        )));
        (
            Consumer {
                inner: scheduler.clone(),
            },
            ThreadLocalProducer { inner: scheduler },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::sync::tests::wait_until;
    use crate::tests::support::{DEADLINE, deadline};
    use std::sync::mpsc;
    use std::thread;

    fn _assert_config_is_send() {
        fn require_send<T: Send>() {}
        require_send::<PollQueue>();
    }

    fn _assert_producer_is_send_sync() {
        fn require_send<T: Send>() {}
        fn require_sync<T: Sync>() {}
        require_send::<Producer>();
        require_sync::<Producer>();
    }

    #[test]
    fn push_and_next() {
        let config = PollQueue::new();
        let (mut consumer, local) = config.local();
        local.push(42);
        assert_eq!(consumer.next(), 42);
    }

    #[test]
    fn fifo_order() {
        let config = PollQueue::new();
        let (mut consumer, local) = config.local();
        local.push(1);
        local.push(2);
        local.push(3);

        assert_eq!(consumer.next(), 1);
        assert_eq!(consumer.next(), 2);
        assert_eq!(consumer.next(), 3);
    }

    #[test]
    fn cross_thread_blocks_until_woken() {
        let _deadline = deadline(DEADLINE);
        let config = PollQueue::new();
        let producer = config.producer();
        let parked = config.parker.parked_probe();
        let (mut consumer, _) = config.local();

        let handle = thread::spawn(move || {
            // Push only once the consumer sleeps, so the push's wake is what releases it.
            wait_until(parked);
            producer.push(99);
        });

        assert_eq!(consumer.next(), 99);
        handle.join().unwrap();
    }

    /// Many park/wake round trips: a producer that woke before linking would sooner or later
    /// leave the consumer parked on an item it never sees.
    #[test]
    fn cross_thread_ping_pong_never_loses_a_wake() {
        let _deadline = deadline(DEADLINE);
        let config = PollQueue::new();
        let producer = config.producer();
        let (mut consumer, _) = config.local();
        let (ack, acked) = mpsc::channel::<()>();
        let handle = thread::spawn(move || {
            for id in 0..20_000 {
                producer.push(id);
                // One in flight at a time, so the consumer parks between pushes.
                acked.recv().unwrap();
            }
        });
        for id in 0..20_000 {
            assert_eq!(consumer.next(), id);
            ack.send(()).unwrap();
        }
        handle.join().unwrap();
    }

    #[test]
    fn mixed_local_and_cross_thread() {
        let config = PollQueue::new();
        let producer = config.producer();
        let (mut consumer, local) = config.local();

        local.push(1);
        local.push(2);

        let handle = thread::spawn(move || {
            producer.push(3);
        });
        handle.join().unwrap();

        assert_eq!(consumer.next(), 1);
        assert_eq!(consumer.next(), 2);
        assert_eq!(consumer.next(), 3);
    }
}
