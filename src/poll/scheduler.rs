//! The poll thread's scheduler: a [TreiberStack] of woken node ids, the thread's timers, and the
//! parker its consumer sleeps on.
//!
//! - [Scheduler]: made with the thread, before it runs; its [Producer] mints cross-thread wakers.
//! - [Scheduler::local], on the poll thread: the [Uninit] consumer, and the [Local] half that
//!   mints the graph's same-thread wakers and timers.
//! - [Uninit::init], once the graph is built: the [Consumer].

mod consumer;
mod local;
mod producer;
#[cfg(test)]
mod tests;
mod treiber;

pub(crate) use consumer::{Consumer, Uninit};
pub(crate) use local::Local;
pub(crate) use producer::Producer;

use crate::poll::timer::Timers;
use crate::wake::Parker;
use treiber::TreiberStack;

use alloc::sync::Arc;

/// `Send`, not `Sync`: it owns the parker, which one thread parks on.
pub(crate) struct Scheduler {
    /// Mints cross-thread wakers; local wakers push onto its stack.
    producer: Producer,
    /// Moves to the poll thread in [Self::local]; producers keep only its waker.
    parker: Parker,
}

impl Scheduler {
    /// O(1): an `Arc` each for the stack (no nodes until init) and the parker.
    pub(crate) fn new() -> Self {
        let parker = Parker::new();
        Self {
            producer: Producer {
                stack: Arc::new(TreiberStack::new()),
                unpark: parker.waker(),
            },
            parker,
        }
    }

    pub(crate) fn producer(&self) -> &Producer {
        &self.producer
    }

    /// The poll thread's halves, both `!Send`. O(1): an `Rc` for the timers, an `Arc` clone.
    pub(crate) fn local(self) -> (Uninit, Local) {
        let timers = Timers::new();
        let uninit = Uninit::new(self.producer.stack.clone(), timers.clone(), self.parker);
        let local = Local {
            producer: self.producer,
            timers,
        };
        (uninit, local)
    }
}

#[cfg(test)]
impl Scheduler {
    /// A scheduler whose graph is ids `0..len`, past their first polls; its local half; a producer
    /// waking it; and whether its consumer is parked, readable from another thread.
    pub(crate) fn started(
        len: usize,
    ) -> (
        Consumer,
        Local,
        Producer,
        impl Fn() -> bool + Send + 'static,
    ) {
        let scheduler = Self::new();
        let producer = scheduler.producer.clone();
        let parked = scheduler.parker.parked_probe();
        let (uninit, local) = scheduler.local();
        let mut consumer = uninit.init(len);
        assert_eq!(consumer.woken(), (0..len).collect::<Vec<_>>());
        (consumer, local, producer, parked)
    }
}
