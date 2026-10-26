//! [Local] — the scheduler's half for building the graph on the poll thread: it mints each
//! node's context. `!Send`, `!Sync`.

use super::Producer;
use super::treiber::TreiberStack;
use crate::poll::Context;
use crate::poll::graph::NodeId;
use crate::poll::timer::{Timer, Timers};
use crate::wake::{self, LocalWake, LocalWaker};

use alloc::sync::Arc;
use core::task;

pub(crate) struct Local {
    pub(super) producer: Producer,
    pub(super) timers: Timers,
}

impl Local {
    /// A new node's context. O(1): an `Arc` for its sync waker, two `Arc` clones, and
    /// [Self::context_with]'s.
    pub(crate) fn context(&self, id: NodeId) -> Context {
        self.context_with(id, self.producer.waker(id))
    }

    /// A reserved node's context, holding `sync`, the waker handed out at reserve. O(1): an `Rc`
    /// for its local waker, and an `Arc` and an `Rc` clone.
    pub(crate) fn context_with(&self, id: NodeId, sync: task::Waker) -> Context {
        Context {
            wake: wake::Waker {
                sync,
                local: LocalWaker::new(Waker {
                    id,
                    stack: self.producer.stack.clone(),
                }),
            },
            timer: Timer::new(id, &self.timers),
        }
    }
}

/// A node's same-thread waker.
struct Waker {
    id: NodeId,
    stack: Arc<TreiberStack>,
}

impl LocalWake for Waker {
    /// O(1): a [TreiberStack::push].
    #[inline]
    fn wake(&self) {
        // No parker wake: the consumer runs on this thread.
        self.stack.push(self.id);
    }
}
