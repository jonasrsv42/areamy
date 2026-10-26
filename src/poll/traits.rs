//! Async poll graph traits.
//!
//! [AsyncParent] is a parent node config that receives an output edge and adds its nodes to the
//! graph. Child nodes build their parents during
//! [GraphBuilder::build](crate::poll::graph::GraphBuilder::build).

use crate::error::Error;
use crate::poll::edge::Sender;
use crate::poll::graph::Graph;
use crate::signal::Origin;
use crate::thread::ThreadId;

/// [crate::graph::Downstream] parameters for a poll builder taking an [AsyncParent].
pub struct OwnedPoll;

/// A parent node in an async graph.
///
/// Each implementor produces one fixed `(OutType, SignalType,
/// ThreadIdType)` triple — a node's output, signal, and home thread
/// are intrinsic to its construction, never varying per call. Encoded
/// as associated types so callers can write bounds that pin only the
/// pieces they care about (e.g. `impl AsyncParent<OutType = Vec<i16>>`)
/// and so projection (`<P as AsyncParent>::OutType`) reads naturally.
///
/// Mirrors [`Pollable`](crate::poll::Pollable)'s `type ThreadId` style.
///
/// Receives an output edge and adds its nodes to the [Graph]. The child creates the edge and
/// calls [AsyncParent::build] during its own
/// [GraphBuilder::build](crate::poll::graph::GraphBuilder::build).
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't be owned by a poll node",
    note = "a work node can't be owned by a poll node: bridge with a Sync input (`.input()` + `pushes`)",
    note = "a poll node with `.output()` can't also be owned (`via`): feed consumers with `pushes` or `tee`"
)]
pub trait AsyncParent<'params>: Send + 'params {
    type OutType;
    type SignalType: Origin;
    type ThreadIdType: ThreadId;

    fn build(
        self: Box<Self>,
        edge: Sender<Self::OutType, Self::SignalType>,
        graph: &mut Graph<'params, Self::ThreadIdType>,
    ) -> Result<(), Error>;
}

/// A boxed parent: picks a stage at runtime (`Some(mic)` or not), one box at build time.
impl<'params, OutType, SignalType, ThreadIdType> AsyncParent<'params>
    for Box<
        dyn AsyncParent<
                'params,
                OutType = OutType,
                SignalType = SignalType,
                ThreadIdType = ThreadIdType,
            > + 'params,
    >
where
    OutType: 'params,
    SignalType: Origin + 'params,
    ThreadIdType: ThreadId,
{
    type OutType = OutType;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        edge: Sender<OutType, SignalType>,
        graph: &mut Graph<'params, ThreadIdType>,
    ) -> Result<(), Error> {
        (*self).build(edge, graph)
    }
}
