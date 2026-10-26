//! The poll graph, built on its thread by [GraphBuilder]s.
//!
//! Two stages, so wakers exist before their thread runs:
//!
//! - [Thread::reserve](crate::poll::Thread::reserve), before the thread runs: reserves a node and
//!   hands out its cross-thread waker.
//! - [Graph], on the poll thread: [Graph::slot] makes a new node's [Slot], [Graph::claim] a
//!   reserved node's, and [Graph::add] places a pollable in it.
//!
//! [Reserved] and [Slot] carry their thread's type and move into their node, so one from another
//! thread type's graph doesn't compile, and none is placed twice. One poll thread per type, so its
//! graph is the only one of that type; a slot of another thread of the same type breaks that
//! contract, and lands unchecked.

mod reservations;
mod slot;
#[cfg(test)]
mod tests;

pub(crate) use reservations::Reservations;
pub use slot::{Reserved, Slot};

use crate::error::Error;
use crate::fatal;
use crate::poll::scheduler::{Consumer, Local, Uninit};
use crate::poll::{Context, Pollable};
use crate::thread::ThreadId;

use alloc::vec::Vec;

/// A node's index in its thread's graph.
pub(crate) type NodeId = usize;

/// A pollable and the context it's polled with.
pub(crate) struct Node<'params, ThreadIdType: ThreadId> {
    pub(crate) pollable: Box<dyn Pollable<ThreadId = ThreadIdType> + 'params>,
    pub(crate) cx: Context,
}

/// The poll loop's nodes, each at its id; a finished one is `None`.
pub(crate) struct Runtime<'params, ThreadIdType: ThreadId> {
    pub(crate) nodes: Vec<Option<Node<'params, ThreadIdType>>>,
}

/// The graph being built on the poll thread. `!Send`.
pub struct Graph<'params, ThreadIdType: ThreadId> {
    /// Mints each node's context.
    local: Local,
    /// Made the consumer of this graph's wakes by [Self::finish].
    uninit: Uninit,
    /// A place per id handed out, reserved ones included; `None` until added.
    nodes: Vec<Option<Node<'params, ThreadIdType>>>,
}

impl<'params, ThreadIdType: ThreadId> Graph<'params, ThreadIdType> {
    /// A new node's slot. O(1) amortized: an `Arc` for its sync waker, two `Arc` clones, and
    /// [Self::claim]'s.
    pub fn slot(&mut self) -> Slot<ThreadIdType> {
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

    /// Places `pollable` in `slot`: the runtime polls it with the slot's context. A slot past the
    /// graph drops it. O(1): a box.
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
        // move. Out of range only for another thread's slot of this type, which breaks the one
        // thread per type contract: a guard, printed and dropped.
        let Some(place) = self.nodes.get_mut(slot.id) else {
            #[cfg(not(feature = "silent"))]
            added_past_the_graph(slot.id, self.nodes.len());
            return;
        };
        *place = Some(node);
    }

    /// The runtime, each node at its id, and the consumer of their wakes. O(n).
    ///
    /// Fails for a slot never added, reserved or made.
    pub(crate) fn finish(self) -> Result<(Runtime<'params, ThreadIdType>, Consumer), Error> {
        if let Some(id) = self.nodes.iter().position(Option::is_none) {
            return Err(fatal!("graph: slot {} is empty", id));
        }
        let consumer = self.uninit.init(self.nodes.len());
        Ok((Runtime { nodes: self.nodes }, consumer))
    }
}

/// Prints a slot added past the graph: one of another thread's of the same type. Cold and out of
/// line, so the add itself needs no stack frame.
#[cfg(not(feature = "silent"))]
#[cold]
#[inline(never)]
fn added_past_the_graph(id: NodeId, n: usize) {
    eprintln!("added slot {id}, past the graph of {n} ids");
}

/// One-shot builder for poll graph nodes.
///
/// Made on any thread ([Send]); [GraphBuilder::build] runs once on the poll thread and adds its
/// nodes to the [Graph]. `'params` bounds how long the pollables it adds may hold borrows captured
/// from the surrounding scope.
pub trait GraphBuilder<'params, ThreadIdType: ThreadId>: Send + 'params {
    fn build(self: Box<Self>, graph: &mut Graph<'params, ThreadIdType>) -> Result<(), Error>;
}
