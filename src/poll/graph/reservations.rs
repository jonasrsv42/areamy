//! [Reservations] — reserves nodes before the poll thread runs.

use super::{Graph, Reserved};
use crate::poll::scheduler::Scheduler;
use crate::thread::ThreadId;

use alloc::vec::Vec;
use core::marker::PhantomData;

/// Reserves nodes before the poll thread runs, for producers on other threads to wake. `Send`.
pub(crate) struct Reservations<ThreadIdType: ThreadId> {
    scheduler: Scheduler,
    /// Ids reserved.
    len: usize,
    _thread: PhantomData<fn() -> ThreadIdType>,
}

impl<ThreadIdType: ThreadId> Reservations<ThreadIdType> {
    pub(crate) fn new() -> Self {
        Self {
            scheduler: Scheduler::new(),
            len: 0,
            _thread: PhantomData,
        }
    }

    /// Reserve the next node. O(1): an `Arc` for its waker, and two `Arc` clones.
    pub(crate) fn reserve(&mut self) -> Reserved<ThreadIdType> {
        let id = self.len;
        self.len += 1;
        Reserved::new(id, self.scheduler.producer().waker(id))
    }

    /// The graph, holding the nodes reserved so far. On the poll thread: it's `!Send`. O(n): a
    /// place per reserved node, and [Scheduler::local].
    pub(crate) fn local<'params>(self) -> Graph<'params, ThreadIdType> {
        let (uninit, local) = self.scheduler.local();
        let mut nodes = Vec::new();
        nodes.resize_with(self.len, || None);
        Graph {
            local,
            uninit,
            nodes,
        }
    }
}
