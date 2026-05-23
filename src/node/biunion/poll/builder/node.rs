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

use super::traits::{ResolveInput, ResolveOutput};
use crate::biunion;
use crate::connect::poll::edge::{Deferred, Edge, Null, Sync};
use crate::connect::poll::wakers::WakerAllocator;
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::Connection;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;
use crate::{Closeable, ThreadId};

/// Builder still holds `&'a mut WakerAllocator` — one or both inputs deferred.
pub struct Allocating<'a>(pub(crate) &'a mut WakerAllocator);

/// Both inputs resolved — allocator borrow released.
pub struct Allocated;

/// Builder input edges grouped together.
pub struct BuilderInput<
    LeftEdge: Edge,
    RightEdge: Edge,
    Left,
    Right,
    SignalType: Origin,
    ThreadIdType: ThreadId,
> {
    pub left: LeftEdge::Input<Left, SignalType, ThreadIdType>,
    pub right: RightEdge::Input<Right, SignalType, ThreadIdType>,
}

/// Biunion node builder.
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub(crate) alloc: AllocState,
    pub(crate) factory: FactoryType,
    pub(crate) input: BuilderInput<LeftEdge, RightEdge, Left, Right, SignalType, ThreadIdType>,
    pub(crate) output: OutEdge::Output<Out, SignalType>,
    pub(crate) _phantom:
        std::marker::PhantomData<(fn() -> Out, fn() -> Left, fn() -> Right, ThreadIdType)>,
}

impl<
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
}

// ============================================================
// Constructor — both inputs deferred
// ============================================================

impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
        Allocating<'a>,
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn deferred(factory: FactoryType, alloc: &'a mut WakerAllocator) -> Self {
        Self {
            alloc: Allocating(alloc),
            factory,
            input: BuilderInput {
                left: Default::default(),
                right: Default::default(),
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// .input::<Side, Edge>() — dispatched via ResolveInput
// ============================================================

macro_rules! impl_input {
    ($left:ty, $right:ty $(, $alloc_lt:lifetime)?) => {
        impl<$($alloc_lt,)? Left, Right, Out, SignalType, ThreadIdType, FactoryType>
            Node<
                Allocating<$($alloc_lt)?>, $left, $right, Deferred, Left, Right, Out,
                SignalType, ThreadIdType, FactoryType,
            >
        where
            SignalType: Origin,
            ThreadIdType: ThreadId,
            FactoryType: BiunionRoutineFactory,
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

impl_input!(Deferred, Deferred, 'a);
impl_input!(Sync, Deferred, 'a);
impl_input!(crate::connect::poll::edge::Async, Deferred, 'a);
impl_input!(Deferred, Sync, 'a);
impl_input!(Deferred, crate::connect::poll::edge::Async, 'a);

// ============================================================
// .output::<Edge>() — dispatched via ResolveOutput
// ============================================================

impl<LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
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
    FactoryType: BiunionRoutineFactory,
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
// Get<dyn Closeable, Left/Right> — Sync inputs with multiplicity
// ============================================================

impl<AllocState, RightEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Get<
        dyn Closeable<DataType = Left, SignalType = SignalType> + Send + std::marker::Sync,
        biunion::Left,
    >
    for Node<
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
    RightEdge: Edge,
    OutEdge: Edge,
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Closeable<DataType = Left, SignalType = SignalType> + Send + std::marker::Sync>,
        Error,
    > {
        Ok(Box::new(self.input.left.edge.sender()))
    }
}

impl<AllocState, LeftEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Get<
        dyn Closeable<DataType = Right, SignalType = SignalType> + Send + std::marker::Sync,
        biunion::Right,
    >
    for Node<
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
    LeftEdge: Edge,
    OutEdge: Edge,
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Closeable<DataType = Right, SignalType = SignalType> + Send + std::marker::Sync>,
        Error,
    > {
        Ok(Box::new(self.input.right.edge.sender()))
    }
}

// ============================================================
// Add<dyn Closeable> — Sync output
// ============================================================

impl<AllocState, LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Add<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync>
    for Node<
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    fn add(
        &mut self,
        connection: Box<
            dyn Closeable<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync,
        >,
    ) -> Result<(), Error> {
        self.output.push(connection);
        Ok(())
    }
}
