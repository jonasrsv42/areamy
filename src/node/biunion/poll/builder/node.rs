//! Biunion node builder with edge kind typestates.
//!
//! Created via [`poll::Thread::biunion`](crate::poll::Thread). Edge kinds
//! are resolved via typestate transitions:
//!
//! - `.input::<Left, Sync>()` / `.input::<Right, Sync>()` → Sync input
//! - `.parent::<Left>(node)` / `.parent::<Right>(node)` → Async input
//! - `.output::<Sync>()` → resolve output to Sync
//!
//! Allocator lifecycle:
//! - `Allocating` — holds `&mut WakerAllocator`, one or both inputs still Deferred
//! - `Allocated` — both inputs resolved, allocator released

use super::options::Options;
use super::traits::{ResolveInput, ResolveOutput};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Add, Get, Outputs};
use crate::node::biunion;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::Sink;
use crate::poll::edge::{Async, Deferred, Edge, Null, Sync};
use crate::poll::input;
use crate::poll::thread::Thread;
use crate::poll::wakers::WakerAllocator;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

/// Builder still holds `&'alloc mut WakerAllocator` — one or both inputs deferred.
pub struct Allocating<'alloc>(pub(crate) &'alloc mut WakerAllocator);

/// Both inputs resolved — allocator borrow released.
pub struct Allocated;

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
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<
    'params,
    AllocState,
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
    pub(crate) alloc: AllocState,
    pub(crate) factory: FactoryType,
    pub(crate) input:
        BuilderInput<'params, LeftEdge, RightEdge, Left, Right, SignalType, ThreadIdType>,
    pub(crate) output: OutEdge::Output<'params, Out, SignalType>,
    pub(crate) options: Options,
    pub(crate) _phantom: PhantomData<(fn() -> Out, fn() -> Left, fn() -> Right, ThreadIdType)>,
}

impl<
    'params,
    AllocState,
    LeftEdge,
    RightEdge,
    OutEdge,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
> Connection
    for Node<
        'params,
        AllocState,
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
    /// - `.input::<Left, Sync>()` / `.input::<Right, Sync>()` → Sync input
    /// - `.parent::<Left>(node)` / `.parent::<Right>(node)` → Async input
    /// - `.output::<Sync>()` → Sync output
    pub fn biunion<Left, Right, Out, SignalType, FactoryType>(
        &mut self,
        factory: FactoryType,
    ) -> Node<
        'params,
        Allocating<'_>,
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

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
        'params,
        Allocating<'alloc>,
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
            alloc: Allocating(alloc),
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
// .input::<Side, Edge>() — dispatched via ResolveInput
// ============================================================

macro_rules! impl_input {
    ($left:ty, $right:ty $(, $alloc_lt:lifetime)?) => {
        impl<$($alloc_lt,)? 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
            Node<
                'params,
                Allocating<$($alloc_lt)?>, $left, $right, Deferred, Left, Right, Out,
                SignalType, ThreadIdType, FactoryType,
            >
        where
            SignalType: Origin,
            ThreadIdType: ThreadId,
            FactoryType: BiunionRoutineFactory<'params>,
            FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
        {
            pub fn input<S, E>(self) -> S::Resolved
            where
                S: ResolveInput<E, Self>,
            {
                S::resolve(self)
            }
        }
    };
}

impl_input!(Deferred, Deferred, 'alloc);
impl_input!(Sync, Deferred, 'alloc);
impl_input!(Async, Deferred, 'alloc);
impl_input!(Deferred, Sync, 'alloc);
impl_input!(Deferred, Async, 'alloc);

// ============================================================
// .output::<Edge>() — dispatched via ResolveOutput
// ============================================================

impl<'params, LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
        'params,
        Allocated,
        LeftEdge,
        RightEdge,
        Deferred,
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
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn output<E>(self) -> E::Resolved
    where
        E: ResolveOutput<Self>,
    {
        E::resolve(self)
    }
}

// ============================================================
// Get<_, Left/Right> — Sync inputs with multiplicity
// ============================================================

impl<
    'params,
    ConnectionType,
    AllocState,
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
        AllocState,
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
    AllocState,
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
        AllocState,
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

impl<
    'params,
    AllocState,
    LeftEdge,
    RightEdge,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
> Add<dyn Sink<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync + 'params>
    for Node<
        'params,
        AllocState,
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

impl<
    'params,
    AllocState,
    LeftEdge,
    RightEdge,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
> Outputs
    for Node<
        'params,
        AllocState,
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
