//! [ReadyQueue] is a thread-safe queue of node IDs ready to be polled.
//!
//! The async thread blocks on [ReadyQueue::blocking_dequeue] until a
//! [NodeWaker] enqueues a node ID.

use crate::error::Error;
use crate::fatal;
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

/// Thread-safe ready queue. Wakers enqueue node IDs, the async thread
/// dequeues and polls the corresponding nodes.
pub struct ReadyQueue {
    inner: Mutex<VecDeque<usize>>,
    signal: Condvar,
}

impl ReadyQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            signal: Condvar::new(),
        }
    }

    /// Enqueue a node ID. Called by [super::waker::NodeWaker] when woken.
    pub fn enqueue(&self, node_id: usize) -> Result<(), Error> {
        let mut queue = self.inner.lock().map_err(|e| fatal!(e))?;
        queue.push_back(node_id);
        self.signal.notify_one();
        Ok(())
    }

    /// Block until a node ID is available, then dequeue it.
    pub fn blocking_dequeue(&self) -> Result<usize, Error> {
        let mut queue = self.inner.lock().map_err(|e| fatal!(e))?;
        while queue.is_empty() {
            queue = self.signal.wait(queue).map_err(|e| fatal!(e))?;
        }
        queue
            .pop_front()
            .ok_or_else(|| fatal!("ReadyQueue: empty after wait"))
    }

    /// Non-blocking dequeue. Returns None if empty.
    #[cfg(test)]
    pub fn try_dequeue(&self) -> Result<Option<usize>, Error> {
        let mut queue = self.inner.lock().map_err(|e| fatal!(e))?;
        Ok(queue.pop_front())
    }

    /// Enqueue multiple node IDs at once.
    /// Uses notify_one() — correct because there is exactly one consumer
    /// (the AsyncThread's poll loop). It will drain all IDs after waking.
    pub fn enqueue_all(&self, ids: impl Iterator<Item = usize>) -> Result<(), Error> {
        let mut queue = self.inner.lock().map_err(|e| fatal!(e))?;
        for id in ids {
            queue.push_back(id);
        }
        self.signal.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn enqueue_and_try_dequeue() {
        let queue = ReadyQueue::new();
        assert_eq!(queue.try_dequeue().unwrap(), None);

        queue.enqueue(5).unwrap();
        assert_eq!(queue.try_dequeue().unwrap(), Some(5));
        assert_eq!(queue.try_dequeue().unwrap(), None);
    }

    #[test]
    fn fifo_order() {
        let queue = ReadyQueue::new();
        queue.enqueue(1).unwrap();
        queue.enqueue(2).unwrap();
        queue.enqueue(3).unwrap();

        assert_eq!(queue.try_dequeue().unwrap(), Some(1));
        assert_eq!(queue.try_dequeue().unwrap(), Some(2));
        assert_eq!(queue.try_dequeue().unwrap(), Some(3));
    }

    #[test]
    fn blocking_dequeue_wakes_on_enqueue() {
        let queue = Arc::new(ReadyQueue::new());
        let queue_clone = queue.clone();

        let handle = thread::spawn(move || queue_clone.blocking_dequeue());

        thread::sleep(std::time::Duration::from_millis(10));

        queue.enqueue(42).unwrap();

        assert_eq!(handle.join().expect("thread panicked").unwrap(), 42);
    }

    #[test]
    fn enqueue_all_batch() {
        let queue = ReadyQueue::new();
        queue.enqueue_all(0..3).unwrap();

        assert_eq!(queue.try_dequeue().unwrap(), Some(0));
        assert_eq!(queue.try_dequeue().unwrap(), Some(1));
        assert_eq!(queue.try_dequeue().unwrap(), Some(2));
        assert_eq!(queue.try_dequeue().unwrap(), None);
    }
}
