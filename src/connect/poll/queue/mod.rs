//! Lock-free MPSC poll queue with two-stage construction.
//!
//! Stage 1 (main thread): [PollQueue] creates [Producer]s for
//! cross-thread wakers. `PollQueue` is `Send` — crosses threads.
//!
//! Stage 2 (async thread): [PollQueue::local] consumes the config
//! and returns [Consumer] + [ThreadLocalProducer] for the async thread.
//!
//! - [Consumer]: blocking dequeue, `!Send`, `!Sync`
//! - [Producer]: enqueue + signal, `Send + Sync + Clone`
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

use ::core::cell::RefCell;
use alloc::rc::Rc;
use alloc::sync::Arc;

pub struct PollQueue {
    inner: Arc<core::VyukovQueue>,
}

impl PollQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(core::VyukovQueue::new()),
        }
    }

    pub fn producer(&self) -> Producer {
        Producer {
            inner: self.inner.clone(),
        }
    }

    /// Consume config and create the async-thread handles.
    /// Must be called on the async thread — returns `!Send` types.
    pub fn local(self) -> (Consumer, ThreadLocalProducer) {
        let scheduler = Rc::new(RefCell::new(scheduler::Scheduler::new(self.inner)));
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
        assert_eq!(consumer.next().unwrap(), 42);
    }

    #[test]
    fn fifo_order() {
        let config = PollQueue::new();
        let (mut consumer, local) = config.local();
        local.push(1);
        local.push(2);
        local.push(3);

        assert_eq!(consumer.next().unwrap(), 1);
        assert_eq!(consumer.next().unwrap(), 2);
        assert_eq!(consumer.next().unwrap(), 3);
    }

    #[test]
    fn cross_thread_blocks_until_signal() {
        let config = PollQueue::new();
        let producer = config.producer();
        let (mut consumer, _) = config.local();

        let handle = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(10));
            producer.push(99).unwrap();
        });

        assert_eq!(consumer.next().unwrap(), 99);
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
            producer.push(3).unwrap();
        });
        handle.join().unwrap();

        assert_eq!(consumer.next().unwrap(), 1);
        assert_eq!(consumer.next().unwrap(), 2);
        assert_eq!(consumer.next().unwrap(), 3);
    }
}
