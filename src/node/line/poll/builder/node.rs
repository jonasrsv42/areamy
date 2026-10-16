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

use super::options::Options;
use super::traits::{ResolveInput, ResolveOutput};
use crate::edge::rotate::Rotate;
use crate::edge::single::Single;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Add, Get, Outputs};
use crate::node::line::poll::factory::{LineRoutineFactory, LineWakers};
use crate::node::line::poll::node;
use crate::node::line::poll::routine::LineRoutine;
use crate::poll::Phase;
use crate::poll::Sink;
use crate::poll::edge::{self, Async, Deferred, Direct, Edge, Null, Sync};
use crate::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::poll::input;
use crate::poll::thread::Thread;
use crate::poll::traits::AsyncParent;
use crate::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

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
    options: Options,
    _phantom: PhantomData<(fn() -> OutType, fn() -> InType, ThreadIdType)>,
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

impl<'params, ThreadIdType: ThreadId> Thread<'params, ThreadIdType> {
    /// Create a node with deferred edge kinds.
    ///
    /// Resolve input and output via wiring methods:
    /// - `.input::<Sync>()` → resolve input to Sync
    /// - `.output::<Sync>()` → resolve output to Sync
    /// - `.parent(node)` → Async input (adds parent)
    /// - consumed by `.parent()` → Deferred output (AsyncParent)
    pub fn line<InType, OutType, SignalType, FactoryType>(
        &mut self,
        factory: FactoryType,
    ) -> Node<'_, 'params, Deferred, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
    where
        SignalType: Origin,
        FactoryType: LineRoutineFactory<'params>,
        FactoryType::Routine: LineRoutine<InType, OutType>,
    {
        Node::deferred(factory, self.waker_allocator())
    }
}

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
        Self::with_options(factory, alloc, Options::default())
    }

    /// A deferred node with `options`, set before any edge exists.
    pub(crate) fn with_options(
        factory: FactoryType,
        alloc: &'alloc mut WakerAllocator,
        options: Options,
    ) -> Self {
        Self {
            alloc,
            factory,
            input: Default::default(),
            output: Null::new(),
            options,
            _phantom: PhantomData,
        }
    }

    /// Resolve input edge. Only [`Sync`] is supported via [`ResolveInput`].
    /// Allocates a sync waker and creates a sync→poll bridge receiver.
    /// Releases the allocator borrow. Output stays Deferred.
    pub fn input<E: ResolveInput<'params, InType, SignalType, ThreadIdType>>(
        self,
    ) -> Node<'static, 'params, E, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
        let (input, alloc) = E::resolve(self.alloc, self.options.bound);
        Node {
            alloc,
            factory: self.factory,
            input,
            output: self.output,
            options: self.options,
            _phantom: PhantomData,
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
            options: self.options,
            _phantom: PhantomData,
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
        let (input, alloc) = E::resolve(self.alloc, self.options.bound);
        Node {
            alloc,
            factory: self.factory,
            input,
            output: self.output,
            options: self.options,
            _phantom: PhantomData,
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
            options: self.options,
            _phantom: PhantomData,
        }
    }

    /// Resolve output to a caller-provided [`Sink`] — the value-resolving
    /// mirror of [`parent`](Self::parent) on the output side. Single
    /// consumer: pushes move into the sink, never cloned.
    pub fn sink(
        self,
        sink: impl Sink<DataType = OutType, SignalType = SignalType> + Send + 'params,
    ) -> Node<'static, 'params, Sync, Direct, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
        Node {
            alloc: (),
            factory: self.factory,
            input: self.input,
            output: Box::new(sink),
            options: self.options,
            _phantom: PhantomData,
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
            options: self.options,
            _phantom: PhantomData,
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
            options: self.options,
            _phantom: PhantomData,
        }
    }

    /// Resolve output to a caller-provided [`Sink`] — the value-resolving
    /// mirror of [`parent`](Self::parent) on the output side. Single
    /// consumer: pushes move into the sink, never cloned.
    pub fn sink(
        self,
        sink: impl Sink<DataType = OutType, SignalType = SignalType> + Send + 'params,
    ) -> Node<'static, 'params, Async, Direct, InType, OutType, SignalType, ThreadIdType, FactoryType>
    {
        Node {
            alloc: (),
            factory: self.factory,
            input: self.input,
            output: Box::new(sink),
            options: self.options,
            _phantom: PhantomData,
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
// Get — Sync input only
// ============================================================

impl<'params, ConnectionType, OutEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Get<ConnectionType>
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
    ConnectionType: Connection + ?Sized,
    OutEdgeType: Edge,
    InType: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
    input::sync::Receiver<InType, SignalType>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input.edge)
    }
}

// ============================================================
// Add<dyn Sink + Send + Sync> — Sync output only
// ============================================================

impl<'params, InEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType>
    Add<dyn Sink<DataType = OutType, SignalType = SignalType> + Send + std::marker::Sync + 'params>
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
            dyn Sink<DataType = OutType, SignalType = SignalType>
                + Send
                + std::marker::Sync
                + 'params,
        >,
    ) -> Result<(), Error> {
        self.output.add(connection);
        Ok(())
    }
}

impl<'params, InEdgeType, InType, OutType, SignalType, ThreadIdType, FactoryType> Outputs
    for Node<'_, 'params, InEdgeType, Sync, InType, OutType, SignalType, ThreadIdType, FactoryType>
where
    InEdgeType: Edge,
    OutType: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType>,
{
    type Sink =
        dyn Sink<DataType = OutType, SignalType = SignalType> + Send + std::marker::Sync + 'params;
}

// ============================================================
// Build — shared phase assembly
// ============================================================

/// Build the three phase pollables for a node with a resolved [`Sync`]
/// input, draining into `output`.
fn sync_input<'params, InType, OutType, SignalType, ThreadIdType, FactoryType, OutputType>(
    factory: FactoryType,
    input: input::sync::Input<InType, SignalType>,
    output: OutputType,
    mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    options: Options,
) -> Result<Graph<'params, ThreadIdType>, Error>
where
    InType: Send + std::marker::Sync + 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
    OutputType: Rotate<DataType = OutType, SignalType = SignalType> + 'params,
{
    let work = allocator.next();
    let output_slot = allocator.next();
    let input_waker = allocator.local_waker(input.slot.id);
    let routine = factory.create(LineWakers {
        input: input_waker.clone(),
        work: work.value.wake.clone(),
        timer: work.value.timer.clone(),
        output: output_slot.value.wake.local.clone(),
    });
    let (input_phase, work_phase, output_phase) = node::Node::phases(
        Phase {
            target: input.edge,
            waker: input_waker,
        },
        Phase {
            target: routine,
            waker: work.value.wake.local,
        },
        output,
        output_slot.value.wake,
        options.budget,
    );
    let nodes = vec![
        GraphNode {
            id: input.slot.id,
            pollable: Box::new(input_phase),
        },
        GraphNode {
            id: work.id,
            pollable: Box::new(work_phase),
        },
        GraphNode {
            id: output_slot.id,
            pollable: Box::new(output_phase),
        },
    ];
    Ok(Graph { allocator, nodes })
}

/// Build the parent graphs and the three phase pollables for a node with an
/// [`Async`] input, draining into `output`.
fn async_input<'params, InType, OutType, SignalType, ThreadIdType, FactoryType, OutputType>(
    factory: FactoryType,
    parents: Vec<
        Box<
            dyn AsyncParent<
                    'params,
                    OutType = InType,
                    SignalType = SignalType,
                    ThreadIdType = ThreadIdType,
                > + 'params,
        >,
    >,
    output: OutputType,
    mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    options: Options,
) -> Result<Graph<'params, ThreadIdType>, Error>
where
    InType: 'static,
    OutType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: LineRoutineFactory<'params>,
    FactoryType::Routine: LineRoutine<InType, OutType> + 'params,
    OutputType: Rotate<DataType = OutType, SignalType = SignalType> + 'params,
{
    let input = allocator.next();
    let edge_waker = input.value.wake.local.clone();
    let mut edges = Vec::new();
    let mut nodes = Vec::new();

    for parent in parents {
        let (sender, receiver) = edge::channel(edge_waker.clone(), options.bound);
        let parent_graph = parent.build(sender, allocator)?;
        allocator = parent_graph.allocator;
        nodes.extend(parent_graph.nodes);
        edges.push(receiver);
    }

    let work = allocator.next();
    let output_slot = allocator.next();
    let routine = factory.create(LineWakers {
        input: input.value.wake.local.clone(),
        work: work.value.wake.clone(),
        timer: work.value.timer.clone(),
        output: output_slot.value.wake.local.clone(),
    });
    let (input_phase, work_phase, output_phase) = node::Node::phases(
        Phase {
            target: edges,
            waker: input.value.wake.local,
        },
        Phase {
            target: routine,
            waker: work.value.wake.local,
        },
        output,
        output_slot.value.wake,
        options.budget,
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
        id: output_slot.id,
        pollable: Box::new(output_phase),
    });
    Ok(Graph { allocator, nodes })
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
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        sync_input(
            self.factory,
            self.input,
            self.output,
            allocator,
            self.options,
        )
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
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        async_input(
            self.factory,
            self.input.parents,
            self.output,
            allocator,
            self.options,
        )
    }
}

/// Terminal with a caller-provided sink: Node<Sync, Direct>
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'static, 'params, Sync, Direct, InType, OutType, SignalType, ThreadIdType, FactoryType>
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
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        sync_input(
            self.factory,
            self.input,
            Single::from(self.output),
            allocator,
            self.options,
        )
    }
}

/// Child with a caller-provided sink: Node<Async, Direct>
impl<'params, InType, OutType, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'static,
        'params,
        Async,
        Direct,
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
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        async_input(
            self.factory,
            self.input.parents,
            Single::from(self.output),
            allocator,
            self.options,
        )
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
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        sync_input(
            self.factory,
            self.input,
            Single::from(self.output),
            allocator,
            self.options,
        )
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
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        async_input(
            self.factory,
            self.input.parents,
            Single::from(self.output),
            allocator,
            self.options,
        )
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
        edge: edge::Sender<OutType, SignalType>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        sync_input(
            self.factory,
            self.input,
            Single::from(edge),
            allocator,
            self.options,
        )
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
        output_edge: edge::Sender<OutType, SignalType>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        async_input(
            self.factory,
            self.input.parents,
            Single::from(output_edge),
            allocator,
            self.options,
        )
    }
}
