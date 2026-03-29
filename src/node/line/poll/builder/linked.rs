//! Linked async line builder. Async input, async output.
//!
//! Owns one or more parent builders. Can itself be owned by a child or
//! another linked node. Implements [Linkable<Parent>] (child gives output
//! edge) and owns parents via [Linkable<Parent>] (gives input edges to parents).
//!
//! Use [LinkedNode::merge] to add additional parents for fan-in.

use crate::connect::poll::edge::AsyncEdge;
use crate::marker::{Connection, Parent};
use crate::node::line::poll::node::AsyncLine;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::thread::poll::spawn::NodeId;
use crate::{Linkable, Pollable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::task::Waker;

/// Builder for an [AsyncLine] with async input and async output.
///
/// Owns one or more parent builders. During [Linkable::link], receives the
/// output edge from the child that owns us, creates input edges for our
/// parents, and cascades the build.
///
/// Use [Self::merge] to add additional parents for fan-in:
///
/// ```text
/// parent_a ─┐
///            ├→ linked → downstream
/// parent_b ─┘
/// ```
#[must_use = "node must be passed to child() or linked()"]
pub struct LinkedNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    node_id: NodeId,
    routine: RoutineType,
    waker: Waker,
    parents: Vec<
        Box<
            dyn Linkable<
                    Parent,
                    Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
                    Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
                >,
        >,
    >,
    _out: std::marker::PhantomData<fn() -> Out>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    LinkedNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: 'static,
    SignalType: Origin + Clone + 'static,
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
            parents: vec![parent],
            _out: std::marker::PhantomData,
        }
    }

    /// Add an additional parent for fan-in. The parent can be any node type
    /// (ParentNode, LinkedNode, etc.) as long as it produces the same data type.
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
    for LinkedNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType> Linkable<Parent>
    for LinkedNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<In, Out> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<Out, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, output_edge: Self::Edge) -> Vec<Self::Node> {
        let node_id = self.node_id;
        let waker = self.waker;
        let mut all_nodes = Vec::new();
        let mut input_edges = Vec::new();

        for parent in self.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            input_edges.push(edge);
        }

        let node = AsyncLine::new(self.routine, input_edges, output_edge);
        all_nodes.push((node_id, Box::new(node)));

        all_nodes
    }
}
