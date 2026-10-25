//! [Producer] — cross-thread producer. `Send + Sync + Clone`.

use super::treiber::TreiberStack;
use crate::poll::marker::NodeId;

use alloc::sync::Arc;
use core::task::Waker;

/// Pushes an id and wakes the consumer.
#[derive(Clone)]
pub struct Producer {
    pub(super) stack: Arc<TreiberStack>,
    /// The consumer's parker.
    pub(super) waker: Waker,
}

impl Producer {
    /// Pushes `id`, from any thread. O(1): a [TreiberStack::push], then the parker's lock (two
    /// RMWs) and a notify when it sleeps.
    ///
    /// Every push wakes: the push that found the stack empty may be preempted before its wake,
    /// and the ids pushed onto it would wait on that producer's priority.
    #[inline]
    pub fn push(&self, id: NodeId) {
        self.stack.push(id);
        self.waker.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wake::Parker;
    use alloc::task::Wake;
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

    fn started(n: usize, waker: Waker) -> Producer {
        let stack = Arc::new(TreiberStack::new());
        stack.init(n);
        Producer { stack, waker }
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
            waker: Waker::from(witness.clone()),
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
}
