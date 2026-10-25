//! GraphBuilder and AsyncParent impls for all biunion edge combinations.

use super::node::Node;
use super::types::Types;
use crate::edge::rotate::Rotate;
use crate::edge::single::Single;
use crate::error::Error;
use crate::graph::Receivable;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::factory::{BiunionRoutineFactory, BiunionWakers};
use crate::node::biunion::poll::node;
use crate::poll::edge::{self, Async, Deferred, Sync};
use crate::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::poll::input::Ready;
use crate::poll::traits::AsyncParent;
use crate::poll::wakers::ThreadLocalWakerAllocator;
use crate::poll::{Budget, Phase};

/// Add a biunion's four phases to `graph`, reading `inputs` and draining into `output`. Its work
/// and output slots are allocated here, after any parents of `inputs` were built.
fn assemble<'params, T, LeftTargetType, RightTargetType, OutputType>(
    factory: T::Factory,
    inputs: Inputs<Ready<LeftTargetType>, Ready<RightTargetType>>,
    output: OutputType,
    budget: Budget,
    mut graph: Graph<'params, T::ThreadId>,
) -> Result<Graph<'params, T::ThreadId>, Error>
where
    T: Types<'params>,
    LeftTargetType: Receivable<DataType = T::Left, SignalType = T::Signal> + 'params,
    RightTargetType: Receivable<DataType = T::Right, SignalType = T::Signal> + 'params,
    OutputType: Rotate<DataType = T::Out, SignalType = T::Signal> + 'params,
{
    let work = graph.allocator.next();
    let output_slot = graph.allocator.next();
    let left_waker = graph.allocator.local_waker(inputs.left.id)?;
    let right_waker = graph.allocator.local_waker(inputs.right.id)?;
    let routine = factory.create(BiunionWakers {
        input: Inputs {
            left: left_waker.clone(),
            right: right_waker.clone(),
        },
        work: work.value.wake.clone(),
        timer: work.value.timer.clone(),
        output: output_slot.value.wake.local.clone(),
    });
    let phases = node::Node::phases(
        Inputs {
            left: Phase {
                target: inputs.left.target,
                waker: left_waker,
            },
            right: Phase {
                target: inputs.right.target,
                waker: right_waker,
            },
        },
        Phase {
            target: routine,
            waker: work.value.wake.local,
        },
        output,
        output_slot.value.wake,
        budget,
    );
    graph.nodes.push(GraphNode {
        id: inputs.left.id,
        pollable: Box::new(phases.inputs.left),
    });
    graph.nodes.push(GraphNode {
        id: inputs.right.id,
        pollable: Box::new(phases.inputs.right),
    });
    graph.nodes.push(GraphNode {
        id: work.id,
        pollable: Box::new(phases.work),
    });
    graph.nodes.push(GraphNode {
        id: output_slot.id,
        pollable: Box::new(phases.output),
    });
    Ok(graph)
}

// ============================================================
// Both Sync inputs
// ============================================================

/// Sync, Sync → Sync output
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Sync, Sync, Sync, T>
where
    T: Types<'params>,
    T::Left: Send + core::marker::Sync,
    T::Right: Send + core::marker::Sync,
    T::Out: Clone + Send + core::marker::Sync,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        assemble::<T, _, _, _>(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        )
    }
}

/// Sync, Sync → Deferred output (sink)
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Sync, Sync, Deferred, T>
where
    T: Types<'params>,
    T::Left: Send + core::marker::Sync,
    T::Right: Send + core::marker::Sync,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(self.output);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}

/// Sync, Sync → Deferred output (consumed as parent)
impl<'params, T> AsyncParent<'params> for Node<'params, Sync, Sync, Deferred, T>
where
    T: Types<'params>,
    T::Left: Send + core::marker::Sync,
    T::Right: Send + core::marker::Sync,
{
    type OutType = T::Out;
    type SignalType = T::Signal;
    type ThreadIdType = T::ThreadId;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<T::Out, T::Signal>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(edge);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}

// ============================================================
// Async left, Sync right
// ============================================================

/// Async, Sync → Sync output
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Async, Sync, Sync, T>
where
    T: Types<'params>,
    T::Right: Send + core::marker::Sync,
    T::Out: Clone + Send + core::marker::Sync,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        assemble::<T, _, _, _>(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        )
    }
}

/// Async, Sync → Deferred output (sink)
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Async, Sync, Deferred, T>
where
    T: Types<'params>,
    T::Right: Send + core::marker::Sync,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(self.output);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}

/// Async, Sync → Deferred output (consumed as parent)
impl<'params, T> AsyncParent<'params> for Node<'params, Async, Sync, Deferred, T>
where
    T: Types<'params>,
    T::Right: Send + core::marker::Sync,
{
    type OutType = T::Out;
    type SignalType = T::Signal;
    type ThreadIdType = T::ThreadId;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<T::Out, T::Signal>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(edge);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}

// ============================================================
// Sync left, Async right
// ============================================================

/// Sync, Async → Sync output
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Sync, Async, Sync, T>
where
    T: Types<'params>,
    T::Left: Send + core::marker::Sync,
    T::Out: Clone + Send + core::marker::Sync,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        assemble::<T, _, _, _>(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        )
    }
}

/// Sync, Async → Deferred output (sink)
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Sync, Async, Deferred, T>
where
    T: Types<'params>,
    T::Left: Send + core::marker::Sync,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        let output = Single::from(self.output);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}

/// Sync, Async → Deferred output (consumed as parent)
impl<'params, T> AsyncParent<'params> for Node<'params, Sync, Async, Deferred, T>
where
    T: Types<'params>,
    T::Left: Send + core::marker::Sync,
{
    type OutType = T::Out;
    type SignalType = T::Signal;
    type ThreadIdType = T::ThreadId;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<T::Out, T::Signal>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        let output = Single::from(edge);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}

// ============================================================
// Both Async inputs
// ============================================================

/// Async, Async → Sync output
impl<'params, T> GraphBuilder<'params, T::ThreadId> for Node<'params, Async, Async, Sync, T>
where
    T: Types<'params>,
    T::Out: Clone + Send + core::marker::Sync,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        assemble::<T, _, _, _>(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        )
    }
}

/// Async, Async → Deferred output (sink)
impl<'params, T: Types<'params>> GraphBuilder<'params, T::ThreadId>
    for Node<'params, Async, Async, Deferred, T>
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        let output = Single::from(self.output);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}

/// Async, Async → Deferred output (consumed as parent)
impl<'params, T: Types<'params>> AsyncParent<'params> for Node<'params, Async, Async, Deferred, T> {
    type OutType = T::Out;
    type SignalType = T::Signal;
    type ThreadIdType = T::ThreadId;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<T::Out, T::Signal>,
        allocator: ThreadLocalWakerAllocator<T::ThreadId>,
    ) -> Result<Graph<'params, T::ThreadId>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        let output = Single::from(edge);
        assemble::<T, _, _, _>(self.factory, inputs, output, self.options.budget, graph)
    }
}
