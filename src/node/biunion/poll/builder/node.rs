//! Biunion node builder with edge kind typestates.
//!
//! Created via [`poll::Thread::biunion`](crate::poll::Thread). Edge kinds
//! are resolved via typestate transitions, in any order:
//!
//! - `.input::<Left>()` / `.input::<Right>()` → Sync input
//! - `up.via(node.at::<Left>())` / `up.via(node.at::<Right>())` → Async input
//! - `.output()` → resolve output to Sync
//!
//! Both input wake slots are reserved at mint, so nodes mint in any order and a node never added
//! leaves its slots empty at build.

use super::options::Options;
use super::traits::{ResolveInput, ResolveOutput};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Add, Get, Outputs};
use crate::node::biunion;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::Sink;
use crate::poll::edge::{Deferred, Edge, Null, Sync};
use crate::poll::input;
use crate::poll::thread::Thread;
use crate::poll::wakers::WakerAllocator;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

/// Builder input edges grouped together.
pub struct BuilderInput<
    'params,
    LeftEdge: Edge,
    RightEdge: Edge,
    Left,
    Right,
    SignalType: Origin,
    ThreadIdType: ThreadId,
> {
    pub left: LeftEdge::Input<'params, Left, SignalType, ThreadIdType>,
    pub right: RightEdge::Input<'params, Right, SignalType, ThreadIdType>,
}

/// Biunion node builder.
#[must_use = "a node does nothing until owned (`via`) or added to a thread"]
pub struct Node<
    'params,
    LeftEdge,
    RightEdge,
    OutEdge,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
> where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub(crate) alloc: Inputs<LeftEdge::Alloc, RightEdge::Alloc>,
    pub(crate) factory: FactoryType,
    pub(crate) input:
        BuilderInput<'params, LeftEdge, RightEdge, Left, Right, SignalType, ThreadIdType>,
    pub(crate) output: OutEdge::Output<'params, Out, SignalType>,
    pub(crate) options: Options,
    pub(crate) _phantom: PhantomData<(fn() -> Out, fn() -> Left, fn() -> Right, ThreadIdType)>,
}

impl<'params, LeftEdge, RightEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Connection
    for Node<
        'params,
        LeftEdge,
        RightEdge,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
}

// ============================================================
// Constructor — both inputs deferred
// ============================================================

impl<'params, ThreadIdType: ThreadId> Thread<'params, ThreadIdType> {
    /// Create a biunion node with two deferred inputs and deferred output.
    ///
    /// Resolve inputs and output via wiring methods:
    /// - `.input::<Left>()` / `.input::<Right>()` → Sync input
    /// - `up.via(node.at::<Left>())` / `up.via(node.at::<Right>())` → Async input
    /// - `.output()` → Sync output
    pub fn biunion<Left, Right, Out, SignalType, FactoryType>(
        &mut self,
        factory: FactoryType,
    ) -> Node<
        'params,
        Deferred,
        Deferred,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
    where
        SignalType: Origin,
        FactoryType: BiunionRoutineFactory<'params>,
        FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    {
        Node::deferred(factory, self.waker_allocator())
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
        'params,
        Deferred,
        Deferred,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub(crate) fn deferred(factory: FactoryType, alloc: &mut WakerAllocator) -> Self {
        Self::with_options(factory, alloc, Options::default())
    }

    /// A deferred node with `options`, set before any edge exists.
    pub(crate) fn with_options(
        factory: FactoryType,
        alloc: &mut WakerAllocator,
        options: Options,
    ) -> Self {
        Self {
            // Reserved at mint: a node never added leaves its slots empty at build.
            alloc: Inputs {
                left: alloc.next(),
                right: alloc.next(),
            },
            factory,
            input: BuilderInput {
                left: Default::default(),
                right: Default::default(),
            },
            output: Null::new(),
            options,
            _phantom: PhantomData,
        }
    }
}

// ============================================================
// .input::<Side>() / .output() — Sync, dispatched via ResolveInput / ResolveOutput
// ============================================================

impl<'params, LeftEdge, RightEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
        'params,
        LeftEdge,
        RightEdge,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    /// Resolve side `S`'s input to [`Sync`]: a sync→poll bridge woken by its reserved slot.
    pub fn input<S>(self) -> <S as ResolveInput<Sync, Self>>::Resolved
    where
        S: ResolveInput<Sync, Self>,
    {
        S::resolve(self)
    }

    /// Resolve the output to [`Sync`].
    pub fn output(self) -> <Sync as ResolveOutput<Self>>::Resolved
    where
        Sync: ResolveOutput<Self>,
    {
        <Sync as ResolveOutput<Self>>::resolve(self)
    }
}

// ============================================================
// Get<_, Left/Right> — Sync inputs with multiplicity
// ============================================================

impl<
    'params,
    ConnectionType,
    RightEdge,
    OutEdge,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
> Get<ConnectionType, biunion::Left>
    for Node<
        'params,
        Sync,
        RightEdge,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    ConnectionType: Connection + ?Sized,
    RightEdge: Edge,
    OutEdge: Edge,
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    input::sync::Receiver<Left, SignalType>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input.left.edge)
    }
}

impl<
    'params,
    ConnectionType,
    LeftEdge,
    OutEdge,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
> Get<ConnectionType, biunion::Right>
    for Node<
        'params,
        LeftEdge,
        Sync,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    ConnectionType: Connection + ?Sized,
    LeftEdge: Edge,
    OutEdge: Edge,
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    input::sync::Receiver<Right, SignalType>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input.right.edge)
    }
}

// ============================================================
// Add<dyn Sink> — Sync output
// ============================================================

impl<'params, LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Add<dyn Sink<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync + 'params>
    for Node<
        'params,
        LeftEdge,
        RightEdge,
        Sync,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    fn add(
        &mut self,
        connection: Box<
            dyn Sink<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync + 'params,
        >,
    ) -> Result<(), Error> {
        self.output.add(connection);
        Ok(())
    }
}

impl<'params, LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType> Outputs
    for Node<
        'params,
        LeftEdge,
        RightEdge,
        Sync,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Sink =
        dyn Sink<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync + 'params;
}
