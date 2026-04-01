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
//!
//! Stores a [RoutineFactory] instead of the routine. The factory is [Send]
//! (crosses threads during spawn). The routine is created on the async
//! thread and does NOT need to be [Send].

use crate::connect::poll::edge::AsyncEdge;
use crate::connect::poll::marker::{Async, AsyncIn, Deferred, EdgeKind, Null, Sync};
use crate::connect::poll::sync_bridge::SyncBridge;
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::{Connection, Parent};
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::thread::poll::spawn::{NodeId, Spawnable};
use crate::{Closeable, Linkable, Pollable, RoutineFactory, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Wakers for the three phases of an async line node.
pub(crate) struct Wakers {
    pub input: (NodeId, std::task::Waker),
    pub work: (NodeId, std::task::Waker),
    pub output: (NodeId, std::task::Waker),
}

/// Unified async node builder.
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: EdgeKind,
    OutEdgeType: EdgeKind,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    wakers: Wakers,
    factory: FactoryType,
    input: InEdgeType::Input<InType, SignalType, ThreadIdType>,
    output: OutEdgeType::Output<OutType, SignalType>,
    _phantom: std::marker::PhantomData<(fn() -> OutType, fn() -> InType, ThreadIdType)>,
}

// ============================================================
// Connection — all variants
// ============================================================

impl<InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType> Connection
    for Node<InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: EdgeKind,
    OutEdgeType: EdgeKind,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
}

// ============================================================
// Constructors
// ============================================================

/// Node<Deferred, Deferred> — unresolved, allocates 2 wakers.
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<Deferred, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    pub fn deferred(
        factory: FactoryType,
        alloc: &mut impl FnMut() -> (NodeId, std::task::Waker),
    ) -> Self {
        Self {
            wakers: Wakers {
                input: alloc(),
                work: alloc(),
                output: alloc(),
            },
            factory,
            input: Default::default(),
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Resolve to Sync input with explicit output kind.
    ///
    /// Async input requires [Self::parent] instead — parents must be provided.
    pub fn typed<OutEdgeType: EdgeKind>(
        self,
    ) -> Node<Sync, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    where
        InType: Send + std::marker::Sync + 'static,
        SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
        OutEdgeType::Output<OutType, SignalType>: Default,
    {
        let input_waker = self.wakers.input.1.clone();
        Node {
            wakers: self.wakers,
            factory: self.factory,
            input: Arc::new(SyncBridge::new(input_waker)),
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// parent() — Deferred→Async transition + Async→Async
// ============================================================

/// First parent on Deferred input: transitions to Async.
impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<Deferred, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    OutEdgeType: EdgeKind,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    pub fn parent(
        self,
        parent: impl Linkable<
            Parent,
            Edge = Rc<RefCell<AsyncEdge<InType, SignalType>>>,
            Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>),
        > + 'static,
    ) -> Node<Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType> {
        let input_waker = self.wakers.input.1.clone();
        Node {
            wakers: self.wakers,
            factory: self.factory,
            input: AsyncIn {
                parents: vec![Box::new(parent)],
                waker: input_waker,
            },
            output: self.output,
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Resolve output kind on Node<Async, Deferred>. Input stays Async.
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    pub fn typed<OutEdgeType: EdgeKind>(
        self,
    ) -> Node<Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    where
        OutEdgeType::Output<OutType, SignalType>: Default,
    {
        Node {
            wakers: self.wakers,
            factory: self.factory,
            input: self.input,
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Additional parent on Async input: adds parent, stays Async.
impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    OutEdgeType: EdgeKind,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
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

impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Get<dyn Closeable<DataType = InType, SignalType = SignalType> + Send + std::marker::Sync>
    for Node<Sync, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    OutEdgeType: EdgeKind,
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
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

impl<InEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Add<dyn Closeable<DataType = OutType, SignalType = SignalType> + Send + std::marker::Sync>
    for Node<InEdgeType, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: EdgeKind,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
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
// Spawnable — calls factory.create() on async thread
// ============================================================

/// Terminal: Node<Sync, Sync>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Spawnable<ThreadIdType>
    for Node<Sync, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            self.output,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        vec![
            (self.wakers.input.0, Box::new(input_phase)),
            (self.wakers.work.0, Box::new(work_phase)),
            (self.wakers.output.0, Box::new(output_phase)),
        ]
    }
}

/// Child: Node<Async, Sync>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Spawnable<ThreadIdType>
    for Node<Async, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let edge_waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut edges = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(edge_waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            edges.push(edge);
        }

        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            edges,
            self.output,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        all_nodes.push((self.wakers.input.0, Box::new(input_phase)));
        all_nodes.push((self.wakers.work.0, Box::new(work_phase)));
        all_nodes.push((self.wakers.output.0, Box::new(output_phase)));
        all_nodes
    }
}

/// Sink: Node<Sync, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Spawnable<ThreadIdType>
    for Node<Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            self.output,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        vec![
            (self.wakers.input.0, Box::new(input_phase)),
            (self.wakers.work.0, Box::new(work_phase)),
            (self.wakers.output.0, Box::new(output_phase)),
        ]
    }
}

/// Sink: Node<Async, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Spawnable<ThreadIdType>
    for Node<Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)> {
        let edge_waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut edges = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(edge_waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            edges.push(edge);
        }

        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            edges,
            self.output,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        all_nodes.push((self.wakers.input.0, Box::new(input_phase)));
        all_nodes.push((self.wakers.work.0, Box::new(work_phase)));
        all_nodes.push((self.wakers.output.0, Box::new(output_phase)));
        all_nodes
    }
}

// ============================================================
// Linkable<Parent> — calls factory.create() on async thread
// ============================================================

/// Node<Sync, Async>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Linkable<Parent>
    for Node<Sync, Async, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, edge: Self::Edge) -> Vec<Self::Node> {
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            edge,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        vec![
            (self.wakers.input.0, Box::new(input_phase)),
            (self.wakers.work.0, Box::new(work_phase)),
            (self.wakers.output.0, Box::new(output_phase)),
        ]
    }
}

/// Node<Sync, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Linkable<Parent>
    for Node<Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, edge: Self::Edge) -> Vec<Self::Node> {
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            edge,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        vec![
            (self.wakers.input.0, Box::new(input_phase)),
            (self.wakers.work.0, Box::new(work_phase)),
            (self.wakers.output.0, Box::new(output_phase)),
        ]
    }
}

/// Node<Async, Async>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Linkable<Parent>
    for Node<Async, Async, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, output_edge: Self::Edge) -> Vec<Self::Node> {
        let edge_waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut input_edges = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(edge_waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            input_edges.push(edge);
        }

        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            input_edges,
            output_edge,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        all_nodes.push((self.wakers.input.0, Box::new(input_phase)));
        all_nodes.push((self.wakers.work.0, Box::new(work_phase)));
        all_nodes.push((self.wakers.output.0, Box::new(output_phase)));
        all_nodes
    }
}

/// Node<Async, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> Linkable<Parent>
    for Node<Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    type Edge = Rc<RefCell<AsyncEdge<OutType, SignalType>>>;
    type Node = (NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>);

    fn link(self: Box<Self>, output_edge: Self::Edge) -> Vec<Self::Node> {
        let edge_waker = self.input.waker;
        let mut all_nodes = Vec::new();
        let mut input_edges = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(AsyncEdge::new(edge_waker.clone())));
            let parent_nodes = parent.link(edge.clone());
            all_nodes.extend(parent_nodes);
            input_edges.push(edge);
        }

        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            input_edges,
            output_edge,
            self.wakers.input.1.clone(),
            self.wakers.work.1,
            self.wakers.output.1,
        );
        all_nodes.push((self.wakers.input.0, Box::new(input_phase)));
        all_nodes.push((self.wakers.work.0, Box::new(work_phase)));
        all_nodes.push((self.wakers.output.0, Box::new(output_phase)));
        all_nodes
    }
}
