//! Child async line builder. Async input, sync output.
//!
//! Owns a parent builder and links it during [Spawnable::spawn],
//! creating the local [AsyncEdge] on the async thread.

use crate::connect::poll::edge::AsyncEdge;
use crate::error::Error;
use crate::graph::Add;
use crate::marker::{Child, Connection, Parent};
use crate::node::line::poll::node::AsyncLine;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::thread::poll::spawn::Spawnable;
use crate::{Closeable, Linkable, Pollable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::task::Waker;

/// Builder for an [AsyncLine] with async input and sync output.
///
/// Always owns a parent builder. During [Spawnable::spawn], creates
/// the local [AsyncEdge] on the async thread and links the parent.
#[must_use = "node must be added to AsyncThread via add()"]
pub struct ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    routine: RoutineType,
    waker: Waker,
    outputs: Vec<Box<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>>,
    parent: Box<
        dyn Linkable<
                Parent,
                Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
                Node = Box<dyn Pollable<ThreadId = ThreadIdType>>,
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
            outputs: Vec::new(),
            parent,
        }
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

// === Linkable<Child>: a parent owns us, gives us input edge ===

impl<In, Out, SignalType, ThreadIdType, RoutineType> Linkable<Child>
    for ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<In, Out> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>;
    type Node = Box<dyn Pollable<ThreadId = ThreadIdType>>;

    fn link(self: Box<Self>, edge: Self::Edge) -> Vec<Self::Node> {
        let node = AsyncLine::new(self.routine, edge, self.outputs);
        vec![Box::new(node)]
    }
}

// === Spawnable: terminal node, owns parent, added to AsyncThread ===

impl<In, Out, SignalType, ThreadIdType, RoutineType> Spawnable<ThreadIdType>
    for ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<In, Out> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<Box<dyn Pollable<ThreadId = ThreadIdType>>> {
        let edge = Rc::new(RefCell::new(AsyncEdge::new(self.waker.clone())));
        let mut nodes = self.parent.link(edge.clone());
        let node = AsyncLine::new(self.routine, edge, self.outputs);
        nodes.push(Box::new(node));
        nodes
    }
}

// === Sync output connections ===

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
