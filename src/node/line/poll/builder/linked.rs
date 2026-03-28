//! Linked async line builder. Async input, async output.
//!
//! Owns a parent builder. Can itself be owned by a child or another linked node.
//! Implements both [Linkable<Parent>] (child gives output edge) and owns
//! a parent via [Linkable<Parent>] (gives input edge to parent).

use crate::connect::poll::edge::AsyncEdge;
use crate::marker::{Connection, Parent};
use crate::node::line::poll::node::AsyncLine;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::{Linkable, Pollable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::task::Waker;

/// Builder for an [AsyncLine] with async input and async output.
///
/// Owns a parent builder. During [Linkable::link], receives the output
/// edge from the child that owns us, creates the input edge for our
/// parent, and cascades the build.
#[must_use = "node must be passed to child() or linked()"]
pub struct LinkedNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    routine: RoutineType,
    waker: Waker,
    parent: Box<
        dyn Linkable<
                Parent,
                Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
                Node = Box<dyn Pollable<ThreadId = ThreadIdType>>,
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
        routine: RoutineType,
        waker: Waker,
        parent: Box<
            dyn Linkable<
                    Parent,
                    Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
                    Node = Box<dyn Pollable<ThreadId = ThreadIdType>>,
                >,
        >,
    ) -> Self {
        Self {
            routine,
            waker,
            parent,
            _out: std::marker::PhantomData,
        }
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

// === Linkable<Parent>: child owns us, gives us output edge ===

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
    type Node = Box<dyn Pollable<ThreadId = ThreadIdType>>;

    fn link(self: Box<Self>, output_edge: Self::Edge) -> Vec<Self::Node> {
        // Create input edge (wakes us when parent pushes)
        let input_edge = Rc::new(RefCell::new(AsyncEdge::new(self.waker)));

        // Link parent — parent gets input_edge as its output
        let mut nodes = self.parent.link(input_edge.clone());

        // Build ourselves with async input + async output
        let node = AsyncLine::new(self.routine, input_edge, output_edge);
        nodes.push(Box::new(node));

        nodes
    }
}
