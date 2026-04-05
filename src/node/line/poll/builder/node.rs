//! Unified async node builder parameterized by edge kind markers.
//!
//! Created via [`AsyncThread::line`](crate::AsyncThread). Edge kinds
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
//! - `Node<Deferred, Deferred>` — fully unresolved, returned by `thread.line()`
//!
//! Stores a [RoutineFactory] instead of the routine. The factory is [Send]
//! (crosses threads during spawn). The routine is created on the async
//! thread and does NOT need to be [Send].

use crate::connect::poll::edge::{
    Async, AsyncIn, Deferred, Edge, Null, PollEdge, Sync, SyncBridge,
};
use crate::connect::poll::graph::{Graph, PollGraphBuilder, PollGraphNode};
use crate::connect::poll::traits::AsyncParent;
use crate::connect::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::Connection;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::{Closeable, RoutineFactory, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Unified async node builder.
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<'a, InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: Edge,
    OutEdgeType: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    alloc: InEdgeType::Alloc<'a>,
    factory: FactoryType,
    input: InEdgeType::Input<InType, SignalType, ThreadIdType>,
    output: OutEdgeType::Output<OutType, SignalType>,
    _phantom: std::marker::PhantomData<(fn() -> OutType, fn() -> InType, ThreadIdType)>,
}

// ============================================================
// Connection — all variants
// ============================================================

impl<InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType> Connection
    for Node<'_, InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: Edge,
    OutEdgeType: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
}

// ============================================================
// Constructors
// ============================================================

/// Node<Deferred, Deferred> — unresolved, no allocation yet.
impl<'a, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'a, Deferred, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    pub fn deferred(factory: FactoryType, alloc: &'a mut WakerAllocator) -> Self {
        Self {
            alloc,
            factory,
            input: Default::default(),
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Resolve to Sync input with explicit output kind.
    ///
    /// Allocates a sync waker for the input node and creates a [SyncBridge].
    /// Releases the allocator borrow.
    pub fn typed<OutEdgeType: Edge>(
        self,
    ) -> Node<'static, Sync, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    where
        InType: Send + std::marker::Sync + 'static,
        SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
        OutEdgeType::Output<OutType, SignalType>: Default,
    {
        let input_slot = self.alloc.next();
        let input_waker = input_slot.value.clone();
        Node {
            alloc: input_slot,
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

/// First parent on Deferred input: transitions to Async. Releases allocator.
impl<'a, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'a, Deferred, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    OutEdgeType: Edge,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    pub fn parent(
        self,
        parent: impl AsyncParent<InType, SignalType, ThreadIdType> + 'static,
    ) -> Node<'static, Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
        Node {
            alloc: (),
            factory: self.factory,
            input: AsyncIn {
                parents: vec![Box::new(parent)],
            },
            output: self.output,
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Resolve output kind on Node<Async, Deferred>. Input stays Async.
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'static, Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    pub fn typed<OutEdgeType: Edge>(
        self,
    ) -> Node<'static, Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    where
        OutEdgeType::Output<OutType, SignalType>: Default,
    {
        Node {
            alloc: (),
            factory: self.factory,
            input: self.input,
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Additional parent on Async input: adds parent, stays Async.
impl<OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'static, Async, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    OutEdgeType: Edge,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: RoutineFactory,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
{
    pub fn parent(
        mut self,
        parent: impl AsyncParent<InType, SignalType, ThreadIdType> + 'static,
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
    for Node<'static, Sync, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    OutEdgeType: Edge,
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
    for Node<'_, InEdgeType, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: Edge,
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
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> PollGraphBuilder<ThreadIdType>
    for Node<'static, Sync, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            self.output,
            self.alloc.value.clone(),
            work.value.sync,
            output.value.sync,
        );
        let nodes = vec![
            PollGraphNode {
                id: self.alloc.id,
                pollable: Box::new(input_phase),
            },
            PollGraphNode {
                id: work.id,
                pollable: Box::new(work_phase),
            },
            PollGraphNode {
                id: output.id,
                pollable: Box::new(output_phase),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Child: Node<Async, Sync>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> PollGraphBuilder<ThreadIdType>
    for Node<'static, Async, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let input = allocator.next();
        let edge_waker = input.value.sync.clone();
        let mut edges = Vec::new();
        let mut nodes = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(PollEdge::new(edge_waker.clone())));
            let parent_graph = parent.build(edge.clone(), allocator)?;
            allocator = parent_graph.allocator;
            nodes.extend(parent_graph.nodes);
            edges.push(edge);
        }

        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            edges,
            self.output,
            input.value.sync,
            work.value.sync,
            output.value.sync,
        );
        nodes.push(PollGraphNode {
            id: input.id,
            pollable: Box::new(input_phase),
        });
        nodes.push(PollGraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        });
        nodes.push(PollGraphNode {
            id: output.id,
            pollable: Box::new(output_phase),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Sink: Node<Sync, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> PollGraphBuilder<ThreadIdType>
    for Node<'static, Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            self.output,
            self.alloc.value.clone(),
            work.value.sync,
            output.value.sync,
        );
        let nodes = vec![
            PollGraphNode {
                id: self.alloc.id,
                pollable: Box::new(input_phase),
            },
            PollGraphNode {
                id: work.id,
                pollable: Box::new(work_phase),
            },
            PollGraphNode {
                id: output.id,
                pollable: Box::new(output_phase),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Sink: Node<Async, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> PollGraphBuilder<ThreadIdType>
    for Node<'static, Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let input = allocator.next();
        let edge_waker = input.value.sync.clone();
        let mut edges = Vec::new();
        let mut nodes = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(PollEdge::new(edge_waker.clone())));
            let parent_graph = parent.build(edge.clone(), allocator)?;
            allocator = parent_graph.allocator;
            nodes.extend(parent_graph.nodes);
            edges.push(edge);
        }

        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            edges,
            self.output,
            input.value.sync,
            work.value.sync,
            output.value.sync,
        );
        nodes.push(PollGraphNode {
            id: input.id,
            pollable: Box::new(input_phase),
        });
        nodes.push(PollGraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        });
        nodes.push(PollGraphNode {
            id: output.id,
            pollable: Box::new(output_phase),
        });
        Ok(Graph { allocator, nodes })
    }
}

// ============================================================
// AsyncParent<OutType, SignalType, ThreadIdType> — calls factory.create() on async thread
// ============================================================

/// Node<Sync, Async>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    AsyncParent<OutType, SignalType, ThreadIdType>
    for Node<'static, Sync, Async, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            edge,
            self.alloc.value.clone(),
            work.value.sync,
            output.value.sync,
        );
        let nodes = vec![
            PollGraphNode {
                id: self.alloc.id,
                pollable: Box::new(input_phase),
            },
            PollGraphNode {
                id: work.id,
                pollable: Box::new(work_phase),
            },
            PollGraphNode {
                id: output.id,
                pollable: Box::new(output_phase),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Node<Sync, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    AsyncParent<OutType, SignalType, ThreadIdType>
    for Node<'static, Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input,
            edge,
            self.alloc.value.clone(),
            work.value.sync,
            output.value.sync,
        );
        let nodes = vec![
            PollGraphNode {
                id: self.alloc.id,
                pollable: Box::new(input_phase),
            },
            PollGraphNode {
                id: work.id,
                pollable: Box::new(work_phase),
            },
            PollGraphNode {
                id: output.id,
                pollable: Box::new(output_phase),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Node<Async, Async>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    AsyncParent<OutType, SignalType, ThreadIdType>
    for Node<'static, Async, Async, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        output_edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let input = allocator.next();
        let edge_waker = input.value.sync.clone();
        let mut input_edges = Vec::new();
        let mut nodes = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(PollEdge::new(edge_waker.clone())));
            let parent_graph = parent.build(edge.clone(), allocator)?;
            allocator = parent_graph.allocator;
            nodes.extend(parent_graph.nodes);
            input_edges.push(edge);
        }

        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            input_edges,
            output_edge,
            input.value.sync,
            work.value.sync,
            output.value.sync,
        );
        nodes.push(PollGraphNode {
            id: input.id,
            pollable: Box::new(input_phase),
        });
        nodes.push(PollGraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        });
        nodes.push(PollGraphNode {
            id: output.id,
            pollable: Box::new(output_phase),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Node<Async, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    AsyncParent<OutType, SignalType, ThreadIdType>
    for Node<'static, Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: RoutineFactory + 'static,
    FactoryType::Routine: AsyncLineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        output_edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let input = allocator.next();
        let edge_waker = input.value.sync.clone();
        let mut input_edges = Vec::new();
        let mut nodes = Vec::new();

        for parent in self.input.parents {
            let edge = Rc::new(RefCell::new(PollEdge::new(edge_waker.clone())));
            let parent_graph = parent.build(edge.clone(), allocator)?;
            allocator = parent_graph.allocator;
            nodes.extend(parent_graph.nodes);
            input_edges.push(edge);
        }

        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create();
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            input_edges,
            output_edge,
            input.value.sync,
            work.value.sync,
            output.value.sync,
        );
        nodes.push(PollGraphNode {
            id: input.id,
            pollable: Box::new(input_phase),
        });
        nodes.push(PollGraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        });
        nodes.push(PollGraphNode {
            id: output.id,
            pollable: Box::new(output_phase),
        });
        Ok(Graph { allocator, nodes })
    }
}
