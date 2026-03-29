//! Child async line builder. Async input, sync output.
//!
//! Owns one or more parent builders and links them during [Spawnable::spawn],
//! creating a local [AsyncEdge] per parent on the async thread.
//!
//! Use [ChildNode::merge] to add additional parents for fan-in.

use crate::connect::poll::edge::AsyncEdge;
use crate::error::Error;
use crate::graph::Add;
use crate::marker::{Connection, Parent};
use crate::node::line::poll::node::AsyncLine;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::thread::poll::spawn::{NodeId, Spawnable};
use crate::{Closeable, Linkable, Pollable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::task::Waker;

/// Builder for an [AsyncLine] with async input and sync output.
///
/// Always owns at least one parent builder. During [Spawnable::spawn],
/// creates one local [AsyncEdge] per parent on the async thread.
///
/// Use [Self::merge] to add additional parents for fan-in:
///
/// ```text
/// parent_a ─┐
///            ├→ child → sync output
/// parent_b ─┘
/// ```
#[must_use = "node must be added to AsyncThread via add()"]
pub struct ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    node_id: NodeId,
    routine: RoutineType,
    waker: Waker,
    outputs: Vec<Box<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>>,
    parents: Vec<
        Box<
            dyn Linkable<
                    Parent,
                    Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
                    Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
                >,
        >,
    >,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    pub fn new(
        node_id: NodeId,
        routine: RoutineType,
        waker: Waker,
        parent: Box<
            dyn Linkable<
                    Parent,
                    Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
                    Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
                >,
        >,
    ) -> Self {
        Self {
            node_id,
            routine,
            waker,
            outputs: Vec::new(),
            parents: vec![parent],
        }
    }

    /// Add an additional parent for fan-in. The parent can be any node type
    /// (ParentNode, LinkedNode, etc.) as long as it produces the same data type.
    ///
    /// All parents are linked during spawn, each getting its own local
    /// [AsyncEdge]. The child drains from all edges, closing only when
    /// all parents have closed.
    pub fn merge(
        mut self,
        parent: impl Linkable<
            Parent,
            Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
            Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
        > + 'static,
    ) -> Self {
        self.parents.push(Box::new(parent));
        self
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType> Connection
    for ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType> Spawnable<ThreadIdType>
    for ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<In, Out> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let node_id = self.node_id;
        let waker = self.waker;
        let mut all_nodes = Vec::new();
        let mut edges = Vec::new();

        for parent in self.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            edges.push(edge);
        }

        let node = AsyncLine::new(self.routine, edges, self.outputs);
        all_nodes.push((node_id, Box::new(node)));
        all_nodes
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>
    for ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    fn add(
        &mut self,
        connection: Box<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>,
    ) -> Result<(), Error> {
        self.outputs.push(connection);
        Ok(())
    }
}
