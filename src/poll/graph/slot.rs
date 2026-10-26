//! [Reserved] and [Slot]: a node's place in its graph, before and after it has a context.

use super::NodeId;
use crate::poll::Context;
use crate::thread::ThreadId;

use core::marker::PhantomData;
use core::task;

/// A node reserved before its thread runs; [Self::waker] is its cross-thread waker. `Send`, not
/// `Clone`: [Graph::claim](super::Graph::claim) takes it.
///
/// Only its own thread type's graph takes it:
///
/// ```compile_fail,E0308
/// use areamy::error::Error;
/// use areamy::poll::{Graph, GraphBuilder, Reserved};
///
/// areamy::thread_id!(Audio, Network);
///
/// struct Claims(Reserved<Audio>);
///
/// impl GraphBuilder<'static, Network> for Claims {
///     fn build(self: Box<Self>, graph: &mut Graph<'static, Network>) -> Result<(), Error> {
///         let _slot = graph.claim(self.0);
///         Ok(())
///     }
/// }
/// ```
pub struct Reserved<ThreadIdType: ThreadId> {
    pub(super) id: NodeId,
    pub(super) waker: task::Waker,
    _thread: PhantomData<fn() -> ThreadIdType>,
}

impl<ThreadIdType: ThreadId> Reserved<ThreadIdType> {
    pub(super) fn new(id: NodeId, waker: task::Waker) -> Self {
        Self {
            id,
            waker,
            _thread: PhantomData,
        }
    }

    /// The node's cross-thread waker: its context holds this very one, so they `will_wake` each
    /// other.
    pub fn waker(&self) -> &task::Waker {
        &self.waker
    }
}

/// A node's id and [Context] on the poll thread. Not `Clone`: [Graph::add](super::Graph::add)
/// takes it.
pub struct Slot<ThreadIdType: ThreadId> {
    pub(super) id: NodeId,
    pub(super) cx: Context,
    _thread: PhantomData<fn() -> ThreadIdType>,
}

impl<ThreadIdType: ThreadId> Slot<ThreadIdType> {
    pub(super) fn new(id: NodeId, cx: Context) -> Self {
        Self {
            id,
            cx,
            _thread: PhantomData,
        }
    }

    /// The context the runtime polls the node with.
    pub fn cx(&self) -> &Context {
        &self.cx
    }
}
