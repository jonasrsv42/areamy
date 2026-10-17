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
use super::types::{PollBiunion, Types};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Add, Get, Outputs};
use crate::node::biunion;
use crate::node::biunion::Inputs;
use crate::poll::Sink;
use crate::poll::edge::{Deferred, Edge, Null, Sync};
use crate::poll::input;
use crate::poll::thread::Thread;
use crate::poll::wakers::WakerAllocator;
use crate::thread::ThreadId;
use std::marker::PhantomData;

/// Builder input edges grouped together.
pub struct BuilderInput<'params, LeftEdge: Edge, RightEdge: Edge, T: Types<'params>> {
    pub left: LeftEdge::Input<'params, T::Left, T::Signal, T::ThreadId>,
    pub right: RightEdge::Input<'params, T::Right, T::Signal, T::ThreadId>,
}

/// Biunion node builder.
#[must_use = "a node does nothing until owned (`via`) or added to a thread"]
pub struct Node<'params, LeftEdge, RightEdge, OutEdge, T>
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
{
    pub(crate) alloc: Inputs<LeftEdge::Alloc, RightEdge::Alloc>,
    pub(crate) factory: T::Factory,
    pub(crate) input: BuilderInput<'params, LeftEdge, RightEdge, T>,
    pub(crate) output: OutEdge::Output<'params, T::Out, T::Signal>,
    pub(crate) options: Options,
    pub(crate) _phantom: PhantomData<fn() -> T>,
}

impl<'params, LeftEdge, RightEdge, OutEdge, T> Connection
    for Node<'params, LeftEdge, RightEdge, OutEdge, T>
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
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
        PollBiunion<Left, Right, Out, SignalType, ThreadIdType, FactoryType>,
    >
    where
        PollBiunion<Left, Right, Out, SignalType, ThreadIdType, FactoryType>:
            Types<'params, Factory = FactoryType>,
    {
        Node::deferred(factory, self.waker_allocator())
    }
}

impl<'params, T: Types<'params>> Node<'params, Deferred, Deferred, Deferred, T> {
    pub(crate) fn deferred(factory: T::Factory, alloc: &mut WakerAllocator) -> Self {
        Self::with_options(factory, alloc, Options::default())
    }

    /// A deferred node with `options`, set before any edge exists.
    pub(crate) fn with_options(
        factory: T::Factory,
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

impl<'params, LeftEdge, RightEdge, OutEdge, T> Node<'params, LeftEdge, RightEdge, OutEdge, T>
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
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

impl<'params, ConnectionType, RightEdge, OutEdge, T> Get<ConnectionType, biunion::Left>
    for Node<'params, Sync, RightEdge, OutEdge, T>
where
    ConnectionType: Connection + ?Sized,
    RightEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
    T::Left: Send + std::marker::Sync,
    input::sync::Receiver<T::Left, T::Signal>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input.left.edge)
    }
}

impl<'params, ConnectionType, LeftEdge, OutEdge, T> Get<ConnectionType, biunion::Right>
    for Node<'params, LeftEdge, Sync, OutEdge, T>
where
    ConnectionType: Connection + ?Sized,
    LeftEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
    T::Right: Send + std::marker::Sync,
    input::sync::Receiver<T::Right, T::Signal>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input.right.edge)
    }
}

// ============================================================
// Add<dyn Sink> — Sync output
// ============================================================

impl<'params, LeftEdge, RightEdge, T>
    Add<dyn Sink<DataType = T::Out, SignalType = T::Signal> + Send + std::marker::Sync + 'params>
    for Node<'params, LeftEdge, RightEdge, Sync, T>
where
    LeftEdge: Edge,
    RightEdge: Edge,
    T: Types<'params>,
    T::Out: Clone + Send + std::marker::Sync,
{
    fn add(
        &mut self,
        connection: Box<
            dyn Sink<DataType = T::Out, SignalType = T::Signal>
                + Send
                + std::marker::Sync
                + 'params,
        >,
    ) -> Result<(), Error> {
        self.output.add(connection);
        Ok(())
    }
}

impl<'params, LeftEdge, RightEdge, T> Outputs for Node<'params, LeftEdge, RightEdge, Sync, T>
where
    LeftEdge: Edge,
    RightEdge: Edge,
    T: Types<'params>,
    T::Out: Clone + Send + std::marker::Sync,
{
    type Sink =
        dyn Sink<DataType = T::Out, SignalType = T::Signal> + Send + std::marker::Sync + 'params;
}
