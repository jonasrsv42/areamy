//! Parent async line builder. Sync input, async output.
//!
//! Linked as [Parent] — a child node owns this builder and provides
//! the output edge during [Linkable::link].

use crate::connect::poll::edge::AsyncEdge;
use crate::connect::poll::sync_bridge::SyncBridge;
use crate::error::Error;
use crate::graph::Get;
use crate::marker::{Connection, Parent};
use crate::node::line::poll::node::AsyncLine;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::thread::poll::spawn::NodeId;
use crate::{Closeable, Linkable, Pollable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;

/// Builder for an [AsyncLine] with sync input and async output.
///
/// Owned by a child builder. During [Linkable::link], receives the
/// output edge (`Rc<RefCell<AsyncEdge>>`) from the child.
#[must_use = "node must be passed to child() or linked()"]
pub struct ParentNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    node_id: NodeId,
    routine: RoutineType,
    input: Arc<SyncBridge<In, SignalType>>,
    _phantom: std::marker::PhantomData<(fn() -> Out, ThreadIdType)>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    ParentNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    pub fn new(node_id: NodeId, routine: RoutineType, waker: Waker) -> Self {
        let input = Arc::new(SyncBridge::new(waker));
        Self {
            node_id,
            routine,
            input,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType> Connection
    for ParentNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType> Linkable<Parent>
    for ParentNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<In, Out> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<Out, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, edge: Self::Edge) -> Vec<Self::Node> {
        let node_id = self.node_id;
        let node = AsyncLine::new(self.routine, self.input, edge);
        vec![(node_id, Box::new(node))]
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync>
    for ParentNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync>, Error>
    {
        Ok(Box::new(self.input.clone()))
    }
}
