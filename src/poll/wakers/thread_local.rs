//! Local (same-thread) waker for the async poll runtime.
//!
//! `Waker` pushes a node's id through its poll thread's `ThreadLocalProducer`.

use crate::poll::marker::NodeId;
use crate::poll::scheduler::ThreadLocalProducer;
use crate::wake::{LocalWake, LocalWaker};

/// `!Send`, `!Sync` behind [LocalWaker].
pub(crate) struct Waker {
    id: NodeId,
    producer: ThreadLocalProducer,
}

impl Waker {
    /// `id`'s local waker. A new one per call, which `will_wake` only its own clones: keep one per
    /// id. O(1): the producer's `Rc` and `Arc` clones, and an `Rc` for the waker.
    pub(crate) fn local(id: NodeId, producer: &ThreadLocalProducer) -> LocalWaker {
        LocalWaker::new(Self {
            id,
            producer: producer.clone(),
        })
    }
}

impl LocalWake for Waker {
    /// O(1): a [ThreadLocalProducer::push].
    #[inline]
    fn wake(&self) {
        // No signal: the consumer runs on this thread.
        let _ = self.producer.push(self.id);
    }
}
