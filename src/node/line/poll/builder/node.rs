//! Unified async node builder parameterized by edge kind markers.
//!
//! Created via [`Thread::line`](crate::poll::Thread). Edge kinds
//! are resolved via typestate transitions:
//!
//! - `.input::<Sync>()` — resolves input to Sync
//! - `.output::<Sync>()` — resolves output to Sync
//! - `.parent(node)` — resolves input to Async (adds parent)
//! - `thread.add(node)` — requires resolved input, Sync or Deferred output
//! - consumed by `.parent()` — requires resolved input, Deferred output
//!
//! Builder type states:
//! - `Node<Deferred, Deferred>` — fully unresolved, returned by `thread.line()`
//! - `Node<Deferred, Sync>` — output resolved first, input pending
//! - `Node<Sync, Deferred>` — sync in, consumed as parent or sink
//! - `Node<Async, Deferred>` — async in, consumed as parent or sink
//! - `Node<Sync, Sync>` — sync in, sync out (terminal)
//! - `Node<Async, Sync>` — async in, sync out (owns parents)
//!
//! `Deferred` output is wired at build time when consumed by `.parent()`.
//!
//! Stores a [LineRoutineFactory] instead of the routine. The factory is [Send]
//! (crosses threads during spawn). The routine is created on the async
//! thread and does NOT need to be [Send].

use super::traits::{ResolveInput, ResolveOutput};
use crate::connect::poll::edge::{Async, AsyncIn, Deferred, Edge, Null, PollEdge, Sync};
use crate::connect::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::connect::poll::traits::AsyncParent;
use crate::connect::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::Connection;
use crate::node::line::poll::factory::LineRoutineFactory;
use crate::node::line::poll::routine::LineRoutine;
use crate::signal::Origin;
use crate::{Closeable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;

/// Unified async node builder.
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<'a, InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: Edge,
    OutEdgeType: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
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
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
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
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
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

    /// Resolve input edge. Only [`Sync`] is supported via [`ResolveInput`].
    /// Allocates a sync waker and creates a [SyncBridge].
    /// Releases the allocator borrow. Output stays Deferred.
    pub fn input<E: ResolveInput<InType, SignalType, ThreadIdType>>(
        self,
    ) -> Node<'static, E, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType> {
        let (input, alloc) = E::resolve(self.alloc);
        Node {
            alloc,
            factory: self.factory,
            input,
            output: self.output,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Resolve output edge. Only [`Sync`] is supported via [`ResolveOutput`].
    /// Input stays Deferred — allocator borrow is preserved.
    pub fn output<E: ResolveOutput<OutType, SignalType>>(
        self,
    ) -> Node<'a, Deferred, E, InType, OutType, SignalType, ThreadIdType, FactoryType> {
        Node {
            alloc: self.alloc,
            factory: self.factory,
            input: self.input,
            output: E::resolve(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// input() on Node<Deferred, Sync> — output already resolved
// ============================================================

/// Resolve input when output is already Sync.
impl<'a, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'a, Deferred, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn input<E: ResolveInput<InType, SignalType, ThreadIdType>>(
        self,
    ) -> Node<'static, E, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType> {
        let (input, alloc) = E::resolve(self.alloc);
        Node {
            alloc,
            factory: self.factory,
            input,
            output: self.output,
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// output() — Deferred→Sync output transition
// ============================================================

/// Resolve output on Node<Sync, Deferred>.
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'static, Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn output<E: ResolveOutput<OutType, SignalType>>(
        self,
    ) -> Node<'static, Sync, E, InType, OutType, SignalType, ThreadIdType, FactoryType> {
        Node {
            alloc: (),
            factory: self.factory,
            input: self.input,
            output: E::resolve(),
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
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn parent(
        self,
        parent: impl AsyncParent<OutType = InType, SignalType = SignalType, ThreadIdType = ThreadIdType>
        + 'static,
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

/// Resolve output on Node<Async, Deferred>. Input stays Async.
impl<InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'static, Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn output<E: ResolveOutput<OutType, SignalType>>(
        self,
    ) -> Node<'static, Async, E, InType, OutType, SignalType, ThreadIdType, FactoryType> {
        Node {
            alloc: (),
            factory: self.factory,
            input: self.input,
            output: E::resolve(),
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
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn parent(
        mut self,
        parent: impl AsyncParent<OutType = InType, SignalType = SignalType, ThreadIdType = ThreadIdType>
        + 'static,
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
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Closeable<DataType = InType, SignalType = SignalType> + Send + std::marker::Sync>,
        Error,
    > {
        Ok(Box::new(self.input.edge.clone()))
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
    FactoryType: LineRoutineFactory,
    FactoryType::Routine: LineRoutine<InType, OutType>,
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
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> GraphBuilder<ThreadIdType>
    for Node<'static, Sync, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: LineRoutineFactory + 'static,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create(output.value.local.clone());
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input.edge,
            self.output,
            allocator.local_waker(self.input.slot.id),
            work.value.local,
            output.value.local,
        );
        let nodes = vec![
            GraphNode {
                id: self.input.slot.id,
                pollable: Box::new(input_phase),
            },
            GraphNode {
                id: work.id,
                pollable: Box::new(work_phase),
            },
            GraphNode {
                id: output.id,
                pollable: Box::new(output_phase),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Child: Node<Async, Sync>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> GraphBuilder<ThreadIdType>
    for Node<'static, Async, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: LineRoutineFactory + 'static,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let input = allocator.next();
        let edge_waker = input.value.local.clone();
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
        let routine = self.factory.create(output.value.local.clone());
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            edges,
            self.output,
            input.value.local,
            work.value.local,
            output.value.local,
        );
        nodes.push(GraphNode {
            id: input.id,
            pollable: Box::new(input_phase),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(output_phase),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Sink: Node<Sync, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> GraphBuilder<ThreadIdType>
    for Node<'static, Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: LineRoutineFactory + 'static,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create(output.value.local.clone());
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input.edge,
            self.output,
            allocator.local_waker(self.input.slot.id),
            work.value.local,
            output.value.local,
        );
        let nodes = vec![
            GraphNode {
                id: self.input.slot.id,
                pollable: Box::new(input_phase),
            },
            GraphNode {
                id: work.id,
                pollable: Box::new(work_phase),
            },
            GraphNode {
                id: output.id,
                pollable: Box::new(output_phase),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Sink: Node<Async, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> GraphBuilder<ThreadIdType>
    for Node<'static, Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: LineRoutineFactory + 'static,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let input = allocator.next();
        let edge_waker = input.value.local.clone();
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
        let routine = self.factory.create(output.value.local.clone());
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            edges,
            self.output,
            input.value.local,
            work.value.local,
            output.value.local,
        );
        nodes.push(GraphNode {
            id: input.id,
            pollable: Box::new(input_phase),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(output_phase),
        });
        Ok(Graph { allocator, nodes })
    }
}

// ============================================================
// AsyncParent — calls factory.create() on async thread
// ============================================================

/// Node<Sync, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> AsyncParent
    for Node<'static, Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: LineRoutineFactory + 'static,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'static,
{
    type OutType = OutType;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let routine = self.factory.create(output.value.local.clone());
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input.edge,
            edge,
            allocator.local_waker(self.input.slot.id),
            work.value.local,
            output.value.local,
        );
        let nodes = vec![
            GraphNode {
                id: self.input.slot.id,
                pollable: Box::new(input_phase),
            },
            GraphNode {
                id: work.id,
                pollable: Box::new(work_phase),
            },
            GraphNode {
                id: output.id,
                pollable: Box::new(output_phase),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Node<Async, Deferred>
impl<InType, OutType, SignalType, ThreadIdType, FactoryType> AsyncParent
    for Node<'static, Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: LineRoutineFactory + 'static,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'static,
{
    type OutType = OutType;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        output_edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let input = allocator.next();
        let edge_waker = input.value.local.clone();
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
        let routine = self.factory.create(output.value.local.clone());
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            input_edges,
            output_edge,
            input.value.local,
            work.value.local,
            output.value.local,
        );
        nodes.push(GraphNode {
            id: input.id,
            pollable: Box::new(input_phase),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(output_phase),
        });
        Ok(Graph { allocator, nodes })
    }
}
