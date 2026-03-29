//! Unified async node builder parameterized by edge kind markers.
//!
//! Created via [`AsyncThread::node`](crate::AsyncThread). Edge kinds
//! are resolved via typestate transitions:
//!
//! - `.typed::<OutEdge>()` — resolves input to Sync, output to turbofish
//! - `.parent(node)` — resolves input to Async (adds parent)
//! - `thread.add(node)` — requires resolved input, Sync or Deferred output
//! - consumed by `.parent()` — requires resolved input, Async or Deferred output
//!
//! Marker combinations:
//! - `Node<Sync, Sync>` — sync in, sync out
//! - `Node<Sync, Async>` — sync in, async out (consumed by downstream `.parent()`)
//! - `Node<Async, Sync>` — async in, sync out (owns parents)
//! - `Node<Async, Async>` — async in, async out (owns parents, consumed by downstream)
//! - `Node<Sync, Deferred>` — sync in, output inferred from usage
//! - `Node<Async, Deferred>` — async in, output inferred (sink if added directly)
//! - `Node<Deferred, Deferred>` — fully unresolved, returned by `thread.node()`

use crate::connect::poll::edge::AsyncEdge;
use crate::connect::poll::marker::{Async, AsyncIn, Deferred, EdgeKind, Null, Sync};
use crate::connect::poll::sync_bridge::SyncBridge;
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::{Connection, Parent, Terminal};
use crate::node::line::poll::node::AsyncLine;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::thread::poll::spawn::{NodeId, Spawnable};
use crate::{Closeable, Linkable, Pollable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Waker;

/// Unified async node builder. Edge kind markers select storage and
/// control which wiring methods are available.
///
/// Created via [`AsyncThread::node`](crate::AsyncThread).
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InEdgeType: EdgeKind,
    OutEdgeType: EdgeKind,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
    node_id: NodeId,
    routine: RoutineType,
    input: InEdgeType::Input<InType, SignalType, ThreadIdType>,
    output: OutEdgeType::Output<OutType, SignalType>,
    _phantom: std::marker::PhantomData<(fn() -> OutType, ThreadIdType)>,
}

// ============================================================
// Connection — all variants
// ============================================================

impl<InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType> Connection
    for Node<InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InEdgeType: EdgeKind,
    OutEdgeType: EdgeKind,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
}

// ============================================================
// Constructors
// ============================================================

/// Node<Sync, _> — sync input, creates SyncBridge from waker.
impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    Node<Sync, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    OutEdgeType: EdgeKind,
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
    OutEdgeType::Output<OutType, SignalType>: Default,
{
    pub fn new(node_id: NodeId, routine: RoutineType, waker: Waker) -> Self {
        Self {
            node_id,
            routine,
            input: Arc::new(SyncBridge::new(waker)),
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Node<Async, _> — async input, takes first parent.
impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    Node<Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    OutEdgeType: EdgeKind,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
    OutEdgeType::Output<OutType, SignalType>: Default,
{
    pub fn new(
        node_id: NodeId,
        routine: RoutineType,
        waker: Waker,
        parent: Box<
            dyn Linkable<
                    Parent,
                    Edge = Rc<RefCell<AsyncEdge<InType, SignalType>>>,
                    Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
                >,
        >,
    ) -> Self {
        Self {
            node_id,
            routine,
            input: AsyncIn {
                parents: vec![parent],
                waker,
            },
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Node<Deferred, Deferred> — unresolved, holds only waker.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType>
    Node<Deferred, Deferred, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    SignalType: Origin,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
    pub fn deferred(node_id: NodeId, routine: RoutineType, waker: Waker) -> Self {
        Self {
            node_id,
            routine,
            input: waker,
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Resolve to Sync input with explicit output kind.
    ///
    /// Async input requires [Self::parent] instead — parents must be provided.
    ///
    /// ```ignore
    /// thread.node(routine).typed::<Async>()  // → Node<Sync, Async>
    /// thread.node(routine).typed::<Sync>()   // → Node<Sync, Sync>
    /// ```
    pub fn typed<OutEdgeType: EdgeKind>(
        self,
    ) -> Node<Sync, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    where
        InType: Send + std::marker::Sync + 'static,
        SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
        OutEdgeType::Output<OutType, SignalType>: Default,
    {
        Node {
            node_id: self.node_id,
            routine: self.routine,
            input: Arc::new(SyncBridge::new(self.input)),
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// merge() — Deferred→Async transition + Async→Async
// ============================================================

/// First merge on Deferred input: transitions to Async.
impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    Node<Deferred, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    OutEdgeType: EdgeKind,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
    pub fn parent(
        self,
        parent: impl Linkable<
            Parent,
            Edge = Rc<RefCell<AsyncEdge<InType, SignalType>>>,
            Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
        > + 'static,
    ) -> Node<Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType> {
        Node {
            node_id: self.node_id,
            routine: self.routine,
            input: AsyncIn {
                parents: vec![Box::new(parent)],
                waker: self.input, // Deferred input IS the waker
            },
            output: self.output,
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Resolve output kind on Node<Async, Deferred>. Input stays Async.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType>
    Node<Async, Deferred, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
    pub fn typed<OutEdgeType: EdgeKind>(
        self,
    ) -> Node<Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    where
        OutEdgeType::Output<OutType, SignalType>: Default,
    {
        Node {
            node_id: self.node_id,
            routine: self.routine,
            input: self.input,
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Additional merge on Async input: adds parent, stays Async.
impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    Node<Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    OutEdgeType: EdgeKind,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
    pub fn parent(
        mut self,
        parent: impl Linkable<
            Parent,
            Edge = Rc<RefCell<AsyncEdge<InType, SignalType>>>,
            Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
        > + 'static,
    ) -> Self {
        self.input.parents.push(Box::new(parent));
        self
    }
}

// ============================================================
// Get<dyn Closeable + Send + Sync> — Sync input only
// ============================================================

impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    Get<dyn Closeable<DataType = InType, SignalType = SignalType> + Send + std::marker::Sync>
    for Node<Sync, OutEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    OutEdgeType: EdgeKind,
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Closeable<DataType = InType, SignalType = SignalType> + Send + std::marker::Sync>,
        Error,
    > {
        Ok(Box::new(self.input.clone()))
    }
}

// ============================================================
// Add<dyn Closeable + Send + Sync> — Sync output only
// ============================================================

impl<InEdgeType, InType, OutType, SignalType, ThreadIdType, RoutineType>
    Add<dyn Closeable<DataType = OutType, SignalType = SignalType> + Send + std::marker::Sync>
    for Node<InEdgeType, Sync, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InEdgeType: EdgeKind,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<InType, OutType>,
{
    fn add(
        &mut self,
        connection: Box<
            dyn Closeable<DataType = OutType, SignalType = SignalType> + Send + std::marker::Sync,
        >,
    ) -> Result<(), Error> {
        self.output.push(connection);
        Ok(())
    }
}

// ============================================================
// Spawnable — Sync output (Node<Sync, Sync> and Node<Async, Sync>)
// ============================================================

/// Terminal: Node<Sync, Sync> — sync in, sync out.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Spawnable<ThreadIdType>
    for Node<Sync, Sync, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let node_id = self.node_id;
        let node = AsyncLine::new(self.routine, self.input, self.output);
        vec![(node_id, Box::new(node))]
    }
}

/// Child: Node<Async, Sync> — async in, sync out. Owns parents.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Spawnable<ThreadIdType>
    for Node<Async, Sync, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let node_id = self.node_id;
        let waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut edges = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            edges.push(edge);
        }

        let node = AsyncLine::new(self.routine, edges, self.output);
        all_nodes.push((node_id, Box::new(node)));
        all_nodes
    }
}

/// Sink: Node<Sync, Deferred> — sync in, no output (sink).
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Spawnable<ThreadIdType>
    for Node<Sync, Deferred, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let node_id = self.node_id;
        let node = AsyncLine::new(self.routine, self.input, self.output);
        vec![(node_id, Box::new(node))]
    }
}

/// Sink: Node<Async, Deferred> — async in, no output (sink). Owns parents.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Spawnable<ThreadIdType>
    for Node<Async, Deferred, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let node_id = self.node_id;
        let waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut edges = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            edges.push(edge);
        }

        let node = AsyncLine::new(self.routine, edges, self.output);
        all_nodes.push((node_id, Box::new(node)));
        all_nodes
    }
}

// ============================================================
// Linkable<Parent> — Async or Deferred output
// Sync input: Node<Sync, Async>, Node<Sync, Deferred>
// Async input: Node<Async, Async>, Node<Async, Deferred>
// ============================================================

/// Node<Sync, Async> — sync in, async out.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Linkable<Parent>
    for Node<Sync, Async, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, edge: Self::Edge) -> Vec<Self::Node> {
        let node_id = self.node_id;
        let node = AsyncLine::new(self.routine, self.input, edge);
        vec![(node_id, Box::new(node))]
    }
}

/// Node<Sync, Deferred> — sync in, output resolved as async by being linked.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Linkable<Parent>
    for Node<Sync, Deferred, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, edge: Self::Edge) -> Vec<Self::Node> {
        let node_id = self.node_id;
        let node = AsyncLine::new(self.routine, self.input, edge);
        vec![(node_id, Box::new(node))]
    }
}

/// Node<Async, Async> — async in, async out. Owns parents.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Linkable<Parent>
    for Node<Async, Async, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, output_edge: Self::Edge) -> Vec<Self::Node> {
        let node_id = self.node_id;
        let waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut input_edges = Vec::new();

        for parent in self.input.parents {
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

/// Node<Async, Deferred> — async in, output resolved as async by being linked.
impl<InType, OutType, SignalType, ThreadIdType, RoutineType> Linkable<Parent>
    for Node<Async, Deferred, InType, OutType, SignalType, ThreadIdType, RoutineType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, output_edge: Self::Edge) -> Vec<Self::Node> {
        let node_id = self.node_id;
        let waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut input_edges = Vec::new();

        for parent in self.input.parents {
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

