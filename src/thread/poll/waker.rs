//! [NodeWaker] implements [core::task::Wake] to enqueue a node into the [ReadyQueue].

use super::ready_queue::ReadyQueue;
use std::sync::Arc;
use std::task::Waker;

/// A waker for a specific node. When woken, enqueues the node's ID
/// into the shared [ReadyQueue].
pub struct NodeWaker {
    node_id: usize,
    ready_queue: Arc<ReadyQueue>,
}

impl NodeWaker {
    pub fn new(node_id: usize, ready_queue: Arc<ReadyQueue>) -> Waker {
        Waker::from(Arc::new(Self {
            node_id,
            ready_queue,
        }))
    }
}

impl std::task::Wake for NodeWaker {
    fn wake(self: Arc<Self>) {
        if let Err(e) = self.ready_queue.enqueue(self.node_id) {
            #[cfg(not(feature = "silent"))]
            eprintln!("NodeWaker: failed to enqueue node {}: {}", self.node_id, e);
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Err(e) = self.ready_queue.enqueue(self.node_id) {
            #[cfg(not(feature = "silent"))]
            eprintln!("NodeWaker: failed to enqueue node {}: {}", self.node_id, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waker_enqueues_node_id() {
        let queue = Arc::new(ReadyQueue::new());
        let waker = NodeWaker::new(42, queue.clone());

        waker.wake_by_ref();

        assert_eq!(queue.try_dequeue().unwrap(), Some(42));
    }

    #[test]
    fn multiple_wakes_enqueue_multiple() {
        let queue = Arc::new(ReadyQueue::new());
        let waker = NodeWaker::new(7, queue.clone());

        waker.wake_by_ref();
        waker.wake_by_ref();

        assert_eq!(queue.try_dequeue().unwrap(), Some(7));
        assert_eq!(queue.try_dequeue().unwrap(), Some(7));
    }

    #[test]
    fn different_wakers_same_queue() {
        let queue = Arc::new(ReadyQueue::new());
        let waker_a = NodeWaker::new(0, queue.clone());
        let waker_b = NodeWaker::new(1, queue.clone());

        waker_b.wake_by_ref();
        waker_a.wake_by_ref();

        assert_eq!(queue.try_dequeue().unwrap(), Some(1));
        assert_eq!(queue.try_dequeue().unwrap(), Some(0));
    }
}
