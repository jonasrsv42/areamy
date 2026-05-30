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
use crate::connect::poll::edge::{Async, Deferred, Edge, Null, PollEdge, Sync};
use crate::connect::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::connect::poll::input;
use crate::connect::poll::traits::AsyncParent;
use crate::connect::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::Connection;
use crate::node::line::poll::factory::{LineRoutineFactory, LineWakers};
use crate::node::line::poll::routine::LineRoutine;
use crate::signal::Origin;
use crate::{Closeable, ThreadId};
use std::cell::RefCell;
use std::rc::Rc;

/// Unified async node builder.
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<
    'alloc,
    'params,
    InEdgeType,
    OutEdgeType,
    InType,
    OutType,
    SignalType,
    ThreadIdType,
    FactoryType,
> where
    InEdgeType: Edge,
    OutEdgeType: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    alloc: InEdgeType::Alloc<'alloc>,
    factory: FactoryType,
    input: InEdgeType::Input<'params, InType, SignalType, ThreadIdType>,
    output: OutEdgeType::Output<'params, OutType, SignalType>,
    _phantom: std::marker::PhantomData<(fn() -> OutType, fn() -> InType, ThreadIdType)>,
}

// ============================================================
// Connection — all variants
// ============================================================

impl<'params, InEdgeType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Connection
    for Node<
        '_,
        'params,
        InEdgeType,
        OutEdgeType,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    InEdgeType: Edge,
    OutEdgeType: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
}

// ============================================================
// Constructors
// ============================================================

/// Node<Deferred, Deferred> — unresolved, no allocation yet.
impl<'alloc, 'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<
        'alloc,
        'params,
        Deferred,
        Deferred,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn deferred(factory: FactoryType, alloc: &'alloc mut WakerAllocator) -> Self {
        Self {
            alloc,
            factory,
            input: Default::default(),
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Resolve input edge. Only [`Sync`] is supported via [`ResolveInput`].
    /// Allocates a sync waker and creates a sync→poll bridge receiver.
    /// Releases the allocator borrow. Output stays Deferred.
    pub fn input<E: ResolveInput<'params, InType, SignalType, ThreadIdType>>(
        self,
    ) -> Node<'static, 'params, E, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
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
    pub fn output<E: ResolveOutput<'params, OutType, SignalType>>(
        self,
    ) -> Node<'alloc, 'params, Deferred, E, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
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
impl<'alloc, 'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'alloc, 'params, Deferred, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn input<E: ResolveInput<'params, InType, SignalType, ThreadIdType>>(
        self,
    ) -> Node<'static, 'params, E, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
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
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'static, 'params, Sync, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn output<E: ResolveOutput<'params, OutType, SignalType>>(
        self,
    ) -> Node<'static, 'params, Sync, E, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
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
impl<'alloc, 'params, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<
        'alloc,
        'params,
        Deferred,
        OutEdgeType,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    OutEdgeType: Edge,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn parent(
        self,
        parent: impl AsyncParent<
            'params,
            OutType = InType,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'params,
    ) -> Node<
        'static,
        'params,
        Async,
        OutEdgeType,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    > {
        Node {
            alloc: (),
            factory: self.factory,
            input: input::r#async::Input {
                parents: vec![Box::new(parent)],
            },
            output: self.output,
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Resolve output on Node<Async, Deferred>. Input stays Async.
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<'static, 'params, Async, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn output<E: ResolveOutput<'params, OutType, SignalType>>(
        self,
    ) -> Node<'static, 'params, Async, E, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
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
impl<'params, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Node<
        'static,
        'params,
        Async,
        OutEdgeType,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    OutEdgeType: Edge,
    InType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    pub fn parent(
        mut self,
        parent: impl AsyncParent<
            'params,
            OutType = InType,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'params,
    ) -> Self {
        self.input.parents.push(Box::new(parent));
        self
    }
}

// ============================================================
// Get<dyn Closeable + Send + Sync> — Sync input only
// ============================================================

// `'params` on the dyn bound: without it the object-lifetime default locks
// the impl to `'static`, and `make_push` from a parent with shorter
// `'params` would fail to select this impl (`Get<T>` is invariant in `T`).
// Reusing the node's `'params` is sufficient — the borrow checker shrinks
// `Node<'params>` covariantly at the call site to match the caller's
// requested lifetime.
impl<'params, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Get<
        dyn Closeable<DataType = InType, SignalType = SignalType>
            + Send
            + std::marker::Sync
            + 'params,
    >
    for Node<
        'static,
        'params,
        Sync,
        OutEdgeType,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    OutEdgeType: Edge,
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    fn get(
        &self,
    ) -> Result<
        Box<
            dyn Closeable<DataType = InType, SignalType = SignalType>
                + Send
                + std::marker::Sync
                + 'params,
        >,
        Error,
    > {
        Ok(Box::new(self.input.edge.sender()))
    }
}

// ============================================================
// Add<dyn Closeable + Send + Sync> — Sync output only
// ============================================================

impl<'params, InEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Add<
        dyn Closeable<DataType = OutType, SignalType = SignalType>
            + Send
            + std::marker::Sync
            + 'params,
    >
    for Node<'_, 'params, InEdgeType, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: Edge,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    fn add(
        &mut self,
        connection: Box<
            dyn Closeable<DataType = OutType, SignalType = SignalType>
                + Send
                + std::marker::Sync
                + 'params,
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
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'static, 'params, Sync, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let input_waker = allocator.local_waker(self.input.slot.id);
        let routine = self.factory.create(LineWakers {
            input: input_waker.clone(),
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input.edge,
            self.output,
            input_waker,
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
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'static, 'params, Async, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InType: 'static,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
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
        let routine = self.factory.create(LineWakers {
            input: input.value.local.clone(),
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
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
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'static,
        'params,
        Sync,
        Deferred,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let input_waker = allocator.local_waker(self.input.slot.id);
        let routine = self.factory.create(LineWakers {
            input: input_waker.clone(),
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input.edge,
            self.output,
            input_waker,
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
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'static,
        'params,
        Async,
        Deferred,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
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
        let routine = self.factory.create(LineWakers {
            input: input.value.local.clone(),
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
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
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'static,
        'params,
        Sync,
        Deferred,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
{
    type OutType = OutType;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let input_waker = allocator.local_waker(self.input.slot.id);
        let routine = self.factory.create(LineWakers {
            input: input_waker.clone(),
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let (input_phase, work_phase, output_phase) = crate::node::line::poll::node::new_phases(
            routine,
            self.input.edge,
            edge,
            input_waker,
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
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'static,
        'params,
        Async,
        Deferred,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
{
    type OutType = OutType;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        output_edge: Rc<RefCell<PollEdge<OutType, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
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
        let routine = self.factory.create(LineWakers {
            input: input.value.local.clone(),
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
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
