//! Unified async node builder parameterized by edge kind markers.
//!
//! Created via [`Thread::line`](crate::poll::Thread). Edge kinds
//! are resolved via typestate transitions:
//!
//! - `.input()` — resolves input to Sync
//! - `.output()` — resolves output to Sync
//! - `up.via(node)` — resolves input to Async (adds parent `up`)
//! - `thread.add(node)` — requires resolved input, Sync or Deferred output
//! - consumed by `node.via(down)` — requires resolved input, Deferred output
//!
//! Builder type states:
//! - `Node<Deferred, Deferred>` — fully unresolved, returned by `thread.line()`
//! - `Node<Deferred, Sync>` — output resolved first, input pending
//! - `Node<Sync, Deferred>` — sync in, consumed as parent or sink
//! - `Node<Async, Deferred>` — async in, consumed as parent or sink
//! - `Node<Sync, Sync>` — sync in, sync out (terminal)
//! - `Node<Async, Sync>` — async in, sync out (owns parents)
//!
//! `Deferred` output is wired at build time when consumed by `node.via(down)`.
//!
//! The input's node is reserved at mint, so nodes mint in any order and a node never added
//! leaves its slot empty at build.
//!
//! Stores a [LineRoutineFactory] instead of the routine. The factory is [Send]
//! (crosses threads during spawn). The routine is created on the async
//! thread and does NOT need to be [Send].

use super::options::Options;
use super::traits::{ResolveInput, ResolveOutput};
use super::types::{PollLine, Types};
use crate::edge::rotate::Rotate;
use crate::edge::single::Single;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Add, Downstream, Get, Outputs, Receivable};
use crate::node::line::poll::factory::{LineRoutineFactory, LineWakers};
use crate::node::line::poll::node;
use crate::poll::Sink;
use crate::poll::edge::{self, Async, Deferred, Direct, Edge, Null, Sync};
use crate::poll::graph::{Graph, GraphBuilder, Reservations};
use crate::poll::input::{self, Ready};
use crate::poll::thread::Thread;
use crate::poll::traits::{AsyncParent, OwnedPoll};
use crate::poll::{Budget, Phase};
use crate::thread::ThreadId;
use core::marker::PhantomData;

/// Unified async node builder.
#[must_use = "a node does nothing until owned (`via`) or added to a thread"]
pub struct Node<'params, InEdgeType, OutEdgeType, T>
where
    InEdgeType: Edge,
    OutEdgeType: Edge,
    T: Types<'params>,
{
    reservation: InEdgeType::Reservation<T::ThreadId>,
    factory: T::Factory,
    input: InEdgeType::Input<'params, T::In, T::Signal, T::ThreadId>,
    output: OutEdgeType::Output<'params, T::Out, T::Signal>,
    options: Options,
    _phantom: PhantomData<fn() -> T>,
}

// ============================================================
// Connection — all variants
// ============================================================

impl<'params, InEdgeType, OutEdgeType, T> Connection for Node<'params, InEdgeType, OutEdgeType, T>
where
    InEdgeType: Edge,
    OutEdgeType: Edge,
    T: Types<'params>,
{
}

// ============================================================
// Constructors
// ============================================================

impl<'params, ThreadIdType: ThreadId> Thread<'params, ThreadIdType> {
    /// Create a node with deferred edge kinds.
    ///
    /// Resolve input and output via wiring methods:
    /// - `.input()` → resolve input to Sync
    /// - `.output()` → resolve output to Sync
    /// - `up.via(node)` → Async input (adds parent `up`)
    /// - consumed by `node.via(down)` → Deferred output (AsyncParent)
    pub fn line<InType, OutType, SignalType, FactoryType>(
        &mut self,
        factory: FactoryType,
    ) -> Node<
        'params,
        Deferred,
        Deferred,
        PollLine<InType, OutType, SignalType, ThreadIdType, FactoryType>,
    >
    where
        PollLine<InType, OutType, SignalType, ThreadIdType, FactoryType>:
            Types<'params, Factory = FactoryType, ThreadId = ThreadIdType>,
    {
        Node::deferred(factory, self.reservations())
    }
}

/// Node<Deferred, Deferred> — unresolved, input slot reserved.
impl<'params, T: Types<'params>> Node<'params, Deferred, Deferred, T> {
    pub(crate) fn deferred(
        factory: T::Factory,
        reservations: &mut Reservations<T::ThreadId>,
    ) -> Self {
        Self::with_options(factory, reservations, Options::default())
    }

    /// A deferred node with `options`, set before any edge exists.
    pub(crate) fn with_options(
        factory: T::Factory,
        reservations: &mut Reservations<T::ThreadId>,
        options: Options,
    ) -> Self {
        Self {
            // Reserved at mint: a node never added leaves its slot empty at build.
            reservation: reservations.next(),
            factory,
            input: Default::default(),
            output: Null::new(),
            options,
            _phantom: PhantomData,
        }
    }

    /// Resolve input to [`Sync`]: a sync→poll bridge receiver woken by the reserved node.
    /// Output stays Deferred.
    pub fn input(self) -> Node<'params, Sync, Deferred, T>
    where
        Sync: ResolveInput<'params, T::In, T::Signal, T::ThreadId>,
    {
        let (input, reservation) =
            <Sync as ResolveInput<'params, T::In, T::Signal, T::ThreadId>>::resolve(
                self.reservation,
                self.options.bound,
            );
        Node {
            reservation,
            factory: self.factory,
            input,
            output: self.output,
            options: self.options,
            _phantom: PhantomData,
        }
    }

    /// Resolve output to [`Sync`]. Input stays Deferred.
    pub fn output(self) -> Node<'params, Deferred, Sync, T>
    where
        Sync: ResolveOutput<'params, T::Out, T::Signal>,
    {
        Node {
            reservation: self.reservation,
            factory: self.factory,
            input: self.input,
            output: <Sync as ResolveOutput<'params, T::Out, T::Signal>>::resolve(),
            options: self.options,
            _phantom: PhantomData,
        }
    }
}

// ============================================================
// input() on Node<Deferred, Sync> — output already resolved
// ============================================================

/// Resolve input when output is already Sync.
impl<'params, T: Types<'params>> Node<'params, Deferred, Sync, T> {
    pub fn input(self) -> Node<'params, Sync, Sync, T>
    where
        Sync: ResolveInput<'params, T::In, T::Signal, T::ThreadId>,
    {
        let (input, reservation) =
            <Sync as ResolveInput<'params, T::In, T::Signal, T::ThreadId>>::resolve(
                self.reservation,
                self.options.bound,
            );
        Node {
            reservation,
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
impl<'params, T> Node<'params, Sync, Deferred, T>
where
    T: Types<'params>,
    T::In: Send + core::marker::Sync,
{
    pub fn output(self) -> Node<'params, Sync, Sync, T>
    where
        Sync: ResolveOutput<'params, T::Out, T::Signal>,
    {
        Node {
            reservation: (),
            factory: self.factory,
            input: self.input,
            output: <Sync as ResolveOutput<'params, T::Out, T::Signal>>::resolve(),
            options: self.options,
            _phantom: PhantomData,
        }
    }

    /// Resolve output to a caller-provided [`Sink`] — the value-resolving
    /// mirror of [`via`](crate::Via::via) on the output side. Single
    /// consumer: pushes move into the sink, never cloned.
    pub fn sink(
        self,
        sink: impl Sink<DataType = T::Out, SignalType = T::Signal> + Send + 'params,
    ) -> Node<'params, Sync, Direct, T> {
        Node {
            reservation: (),
            factory: self.factory,
            input: self.input,
            output: Box::new(sink),
            options: self.options,
            _phantom: PhantomData,
        }
    }
}

/// Resolve output on Node<Async, Deferred>. Input stays Async.
impl<'params, T: Types<'params>> Node<'params, Async, Deferred, T> {
    pub fn output(self) -> Node<'params, Async, Sync, T>
    where
        Sync: ResolveOutput<'params, T::Out, T::Signal>,
    {
        Node {
            reservation: self.reservation,
            factory: self.factory,
            input: self.input,
            output: <Sync as ResolveOutput<'params, T::Out, T::Signal>>::resolve(),
            options: self.options,
            _phantom: PhantomData,
        }
    }

    /// Resolve output to a caller-provided [`Sink`] — the value-resolving
    /// mirror of [`via`](crate::Via::via) on the output side. Single
    /// consumer: pushes move into the sink, never cloned.
    pub fn sink(
        self,
        sink: impl Sink<DataType = T::Out, SignalType = T::Signal> + Send + 'params,
    ) -> Node<'params, Async, Direct, T> {
        Node {
            reservation: self.reservation,
            factory: self.factory,
            input: self.input,
            output: Box::new(sink),
            options: self.options,
            _phantom: PhantomData,
        }
    }
}

// ============================================================
// own() — `up.via(line)`: Deferred→Async transition + Async→Async
// ============================================================

/// `up.via(line)`: first parent, input Deferred → Async.
impl<'params, OutEdgeType, T, UpstreamType> Downstream<UpstreamType, T::In, OwnedPoll>
    for Node<'params, Deferred, OutEdgeType, T>
where
    OutEdgeType: Edge,
    T: Types<'params>,
    UpstreamType:
        AsyncParent<'params, OutType = T::In, SignalType = T::Signal, ThreadIdType = T::ThreadId>,
{
    type Out = Node<'params, Async, OutEdgeType, T>;

    fn own(self, upstream: UpstreamType) -> Result<Self::Out, Error> {
        Ok(Node {
            reservation: self.reservation,
            factory: self.factory,
            input: input::r#async::Input {
                parents: vec![Box::new(upstream)],
            },
            output: self.output,
            options: self.options,
            _phantom: PhantomData,
        })
    }
}

/// `up.via(line)`: further parent (fan-in), input stays Async.
impl<'params, OutEdgeType, T, UpstreamType> Downstream<UpstreamType, T::In, OwnedPoll>
    for Node<'params, Async, OutEdgeType, T>
where
    OutEdgeType: Edge,
    T: Types<'params>,
    UpstreamType:
        AsyncParent<'params, OutType = T::In, SignalType = T::Signal, ThreadIdType = T::ThreadId>,
{
    type Out = Self;

    fn own(mut self, upstream: UpstreamType) -> Result<Self, Error> {
        self.input.parents.push(Box::new(upstream));
        Ok(self)
    }
}

// ============================================================
// Get — Sync input only
// ============================================================

impl<'params, ConnectionType, OutEdgeType, T> Get<ConnectionType>
    for Node<'params, Sync, OutEdgeType, T>
where
    ConnectionType: Connection + ?Sized,
    OutEdgeType: Edge,
    T: Types<'params>,
    T::In: Send + core::marker::Sync,
    input::sync::Receiver<T::In, T::Signal>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input.edge)
    }
}

// ============================================================
// Add<dyn Sink + Send + Sync> — Sync output only
// ============================================================

impl<'params, InEdgeType, T>
    Add<dyn Sink<DataType = T::Out, SignalType = T::Signal> + Send + core::marker::Sync + 'params>
    for Node<'params, InEdgeType, Sync, T>
where
    InEdgeType: Edge,
    T: Types<'params>,
    T::Out: Clone + Send + core::marker::Sync,
{
    fn add(
        &mut self,
        connection: Box<
            dyn Sink<DataType = T::Out, SignalType = T::Signal>
                + Send
                + core::marker::Sync
                + 'params,
        >,
    ) -> Result<(), Error> {
        self.output.add(connection);
        Ok(())
    }
}

impl<'params, InEdgeType, T> Outputs for Node<'params, InEdgeType, Sync, T>
where
    InEdgeType: Edge,
    T: Types<'params>,
    T::Out: Clone + Send + core::marker::Sync,
{
    type Sink =
        dyn Sink<DataType = T::Out, SignalType = T::Signal> + Send + core::marker::Sync + 'params;
}

// ============================================================
// Build — shared phase assembly
// ============================================================

/// Add a line node's three phases to `graph`, reading `input` and draining into `output`. Its
/// work and output slots are made here, after any parents of `input` were built.
fn assemble<'params, T, TargetType, OutputType>(
    factory: T::Factory,
    input: Ready<TargetType, T::ThreadId>,
    output: OutputType,
    budget: Budget,
    graph: &mut Graph<'params, T::ThreadId>,
) where
    T: Types<'params>,
    TargetType: Receivable<DataType = T::In, SignalType = T::Signal> + 'params,
    OutputType: Rotate<DataType = T::Out, SignalType = T::Signal> + 'params,
{
    let work = graph.next();
    let output_slot = graph.next();
    let input_waker = input.slot.cx().wake.local.clone();
    let routine = factory.create(LineWakers {
        input: input_waker.clone(),
        work: work.cx().clone(),
        output: output_slot.cx().wake.local.clone(),
    });
    let (input_phase, work_phase, output_phase) = node::Node::phases(
        Phase {
            target: input.target,
            waker: input_waker,
        },
        Phase {
            target: routine,
            waker: work.cx().wake.local.clone(),
        },
        output,
        output_slot.cx().wake.clone(),
        budget,
    );
    graph.add(input.slot, input_phase);
    graph.add(work, work_phase);
    graph.add(output_slot, output_phase);
}

// ============================================================
// GraphBuilder — calls factory.create() on the poll thread
// ============================================================

/// Terminal: Node<Sync, Sync>
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Sync, Sync, T>
where
    T: Types<'params>,
    T::In: Send + core::marker::Sync,
    T::Out: Clone + Send + core::marker::Sync,
{
    fn build(self: Box<Self>, graph: &mut Graph<'params, T::ThreadId>) -> Result<(), Error> {
        let input = self.input.ready(graph);
        assemble::<T, _, _>(self.factory, input, self.output, self.options.budget, graph);
        Ok(())
    }
}

/// Child: Node<Async, Sync>
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Async, Sync, T>
where
    T: Types<'params>,
    T::Out: Clone + Send + core::marker::Sync,
{
    fn build(self: Box<Self>, graph: &mut Graph<'params, T::ThreadId>) -> Result<(), Error> {
        let input = self
            .input
            .ready(self.reservation, self.options.bound, graph)?;
        assemble::<T, _, _>(self.factory, input, self.output, self.options.budget, graph);
        Ok(())
    }
}

/// Terminal with a caller-provided sink: Node<Sync, Direct>
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Sync, Direct, T>
where
    T: Types<'params>,
    T::In: Send + core::marker::Sync,
{
    fn build(self: Box<Self>, graph: &mut Graph<'params, T::ThreadId>) -> Result<(), Error> {
        let input = self.input.ready(graph);
        let output = Single::from(self.output);
        assemble::<T, _, _>(self.factory, input, output, self.options.budget, graph);
        Ok(())
    }
}

/// Child with a caller-provided sink: Node<Async, Direct>
impl<'params, T: Types<'params>> GraphBuilder<'params, T::ThreadId>
    for Node<'params, Async, Direct, T>
{
    fn build(self: Box<Self>, graph: &mut Graph<'params, T::ThreadId>) -> Result<(), Error> {
        let input = self
            .input
            .ready(self.reservation, self.options.bound, graph)?;
        let output = Single::from(self.output);
        assemble::<T, _, _>(self.factory, input, output, self.options.budget, graph);
        Ok(())
    }
}

/// Sink: Node<Sync, Deferred>
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Sync, Deferred, T>
where
    T: Types<'params>,
    T::In: Send + core::marker::Sync,
{
    fn build(self: Box<Self>, graph: &mut Graph<'params, T::ThreadId>) -> Result<(), Error> {
        let input = self.input.ready(graph);
        let output = Single::from(self.output);
        assemble::<T, _, _>(self.factory, input, output, self.options.budget, graph);
        Ok(())
    }
}

/// Sink: Node<Async, Deferred>
impl<'params, T: Types<'params>> GraphBuilder<'params, T::ThreadId>
    for Node<'params, Async, Deferred, T>
{
    fn build(self: Box<Self>, graph: &mut Graph<'params, T::ThreadId>) -> Result<(), Error> {
        let input = self
            .input
            .ready(self.reservation, self.options.bound, graph)?;
        let output = Single::from(self.output);
        assemble::<T, _, _>(self.factory, input, output, self.options.budget, graph);
        Ok(())
    }
}

// ============================================================
// AsyncParent — calls factory.create() on async thread
// ============================================================

/// Node<Sync, Deferred>
impl<'params, T> AsyncParent<'params> for Node<'params, Sync, Deferred, T>
where
    T: Types<'params>,
    T::In: Send + core::marker::Sync,
{
    type OutType = T::Out;
    type SignalType = T::Signal;
    type ThreadIdType = T::ThreadId;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<T::Out, T::Signal>,
        graph: &mut Graph<'params, T::ThreadId>,
    ) -> Result<(), Error> {
        let input = self.input.ready(graph);
        let output = Single::from(edge);
        assemble::<T, _, _>(self.factory, input, output, self.options.budget, graph);
        Ok(())
    }
}

/// Node<Async, Deferred>
impl<'params, T: Types<'params>> AsyncParent<'params> for Node<'params, Async, Deferred, T> {
    type OutType = T::Out;
    type SignalType = T::Signal;
    type ThreadIdType = T::ThreadId;

    fn build(
        self: Box<Self>,
        output_edge: edge::Sender<T::Out, T::Signal>,
        graph: &mut Graph<'params, T::ThreadId>,
    ) -> Result<(), Error> {
        let input = self
            .input
            .ready(self.reservation, self.options.bound, graph)?;
        let output = Single::from(output_edge);
        assemble::<T, _, _>(self.factory, input, output, self.options.budget, graph);
        Ok(())
    }
}
