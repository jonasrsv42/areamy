//! [Producer] — cross-thread producer, and the cross-thread waker it backs. `Send + Sync + Clone`.

use super::treiber::TreiberStack;
use crate::poll::graph::NodeId;

use alloc::sync::Arc;
use alloc::task::Wake;
use core::task;

/// Pushes an id and wakes the consumer.
#[derive(Clone)]
pub(crate) struct Producer {
    pub(super) stack: Arc<TreiberStack>,
    /// Wakes the consumer's parker.
    pub(super) unpark: task::Waker,
}

impl Producer {
    /// Pushes `id`, from any thread. O(1): a [TreiberStack::push], then the parker's lock (two
    /// RMWs) and a notify when it sleeps.
    ///
    /// Every push wakes: the push that found the stack empty may be preempted before its wake,
    /// and the ids pushed onto it would wait on that producer's priority.
    #[inline]
    pub(crate) fn push(&self, id: NodeId) {
        self.stack.push(id);
        self.unpark.wake_by_ref();
    }

    /// `id`'s cross-thread waker. O(1): an `Arc` for the waker, and this producer's two `Arc`
    /// clones.
    pub(crate) fn waker(&self, id: NodeId) -> task::Waker {
        task::Waker::from(Arc::new(Waker {
            id,
            producer: self.clone(),
        }))
    }
}

/// A node's cross-thread waker.
struct Waker {
    id: NodeId,
    producer: Producer,
}

impl Wake for Waker {
    /// O(1): [Self::wake_by_ref].
    #[inline]
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    /// O(1): a [Producer::push].
    #[inline]
    fn wake_by_ref(self: &Arc<Self>) {
        self.producer.push(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wake::Parker;
    use core::sync::atomic::{AtomicUsize, Ordering};

    /// Stands in for the parker: takes when woken, as the consumer would.
    struct Witness {
        stack: Arc<TreiberStack>,
        taken: AtomicUsize,
    }

    impl Wake for Witness {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let mut cursor = self.stack.take();
            if let Some(id) = self.stack.next(&mut cursor) {
                self.taken.store(id, Ordering::SeqCst);
            }
        }
    }

    fn started(n: usize, unpark: task::Waker) -> Producer {
        let stack = Arc::new(TreiberStack::new());
        stack.init(n);
        Producer { stack, unpark }
    }

    /// Push, then wake: a consumer woken first could find nothing and sleep on it.
    #[test]
    fn a_wake_finds_its_id_pushed() {
        let stack = Arc::new(TreiberStack::new());
        stack.init(8);
        let witness = Arc::new(Witness {
            stack: stack.clone(),
            taken: AtomicUsize::new(0),
        });
        let producer = Producer {
            stack,
            unpark: task::Waker::from(witness.clone()),
        };
        producer.push(3);
        assert_eq!(witness.taken.load(Ordering::SeqCst), 3);
    }

    /// First, linked and coalesced alike.
    #[test]
    fn every_push_wakes_the_parker() {
        let parker = Parker::new();
        let producer = started(8, parker.waker());
        for id in [1, 2, 1] {
            producer.push(id);
            assert!(parker.take_woken(), "push of {id}");
        }
    }

    #[test]
    fn a_nodes_waker_pushes_its_id_and_wakes_the_parker() {
        let parker = Parker::new();
        let producer = started(8, parker.waker());
        let waker = producer.waker(5);
        waker.wake_by_ref();
        assert!(parker.take_woken());
        waker.wake();
        let mut walk = producer.stack.take();
        assert_eq!(producer.stack.next(&mut walk), Some(5));
        assert_eq!(producer.stack.next(&mut walk), None);
    }
}
