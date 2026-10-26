//! The poll graph, built on its thread by [GraphBuilder]s.
//!
//! Two stages, so wakers exist before their thread runs:
//!
//! - [Thread::reserve](crate::poll::Thread::reserve), before the thread runs: reserves a node and
//!   hands out its cross-thread waker.
//! - [Graph], on the poll thread: [Graph::next] makes a new node's [Slot], [Graph::claim] a
//!   reserved node's, and [Graph::add] places a pollable in it.
//!
//! [Reserved] and [Slot] carry their thread's type and move into their node, so one from another
//! thread type's graph doesn't compile, and none is placed twice. One poll thread per type, so its
//! graph is the only one of that type.

mod reservations;
mod slot;
#[cfg(test)]
mod tests;

pub(crate) use reservations::Reservations;
pub use slot::{Reserved, Slot};

use crate::error::Error;
use crate::fatal;
use crate::poll::Pollable;
use crate::poll::scheduler::{Consumer, Local, Uninit};
use crate::poll::thread::runtime::{Node, Runtime};
use crate::thread::ThreadId;

use alloc::vec::Vec;

/// A node's index in its thread's graph.
pub(crate) type NodeId = usize;

/// The graph being built on the poll thread. `!Send`.
pub struct Graph<'params, ThreadIdType: ThreadId> {
    /// Mints each node's wakers and timer.
    local: Local,
    /// Made the consumer of this graph's wakes by [Self::finish].
    uninit: Uninit,
    /// A place per id handed out, reserved ones included; `None` until added.
    nodes: Vec<Option<Node<'params, ThreadIdType>>>,
}

impl<'params, ThreadIdType: ThreadId> Graph<'params, ThreadIdType> {
    /// A new node's slot. O(1) amortized: an `Arc` for its sync waker, and [Self::claim]'s.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Slot<ThreadIdType> {
        let id = self.nodes.len();
        self.nodes.push(None);
        Slot::new(id, self.local.context(id))
    }

    /// A reserved node's slot, its context holding the waker handed out at reserve. O(1): an `Rc`
    /// for its local waker, and an `Arc` and an `Rc` clone.
    pub fn claim(&self, reserved: Reserved<ThreadIdType>) -> Slot<ThreadIdType> {
        let cx = self.local.context_with(reserved.id, reserved.waker);
        Slot::new(reserved.id, cx)
    }

    /// Places `pollable` in `slot`: the runtime polls it with the slot's context. O(1): a box.
    pub fn add(
        &mut self,
        slot: Slot<ThreadIdType>,
        pollable: impl Pollable<ThreadId = ThreadIdType> + 'params,
    ) {
        let node = Node {
            pollable: Box::new(pollable),
            cx: slot.cx,
        };
        // In range and unplaced: the thread type's one graph hands out each id once, and slots
        // move.
        if let Some(place) = self.nodes.get_mut(slot.id) {
            *place = Some(node);
        }
    }

    /// The runtime, each node at its id, and the consumer of their wakes. O(n).
    ///
    /// Fails for a node reserved but never added.
    pub(crate) fn finish(self) -> Result<(Runtime<'params, ThreadIdType>, Consumer), Error> {
        if let Some(id) = self.nodes.iter().position(Option::is_none) {
            return Err(fatal!("graph: slot {} is empty", id));
        }
        let consumer = self.uninit.init(self.nodes.len());
        Ok((Runtime { nodes: self.nodes }, consumer))
    }
}

/// One-shot builder for poll graph nodes.
///
/// Made on any thread ([Send]); [GraphBuilder::build] runs once on the poll thread and adds its
/// nodes to the [Graph]. `'params` bounds how long the pollables it adds may hold borrows captured
/// from the surrounding scope.
pub trait GraphBuilder<'params, ThreadIdType: ThreadId>: Send + 'params {
    fn build(self: Box<Self>, graph: &mut Graph<'params, ThreadIdType>) -> Result<(), Error>;
}
