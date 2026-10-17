//! GraphBuilder and AsyncParent impls for all biunion edge combinations.

use super::node::Node;
use crate::edge::rotate::Rotate;
use crate::edge::single::Single;
use crate::error::Error;
use crate::graph::Receivable;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::factory::{BiunionRoutineFactory, BiunionWakers};
use crate::node::biunion::poll::node;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::edge::{self, Async, Deferred, Sync};
use crate::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::poll::input::Ready;
use crate::poll::traits::AsyncParent;
use crate::poll::wakers::ThreadLocalWakerAllocator;
use crate::poll::{Budget, Phase};
use crate::signal::Origin;
use crate::thread::ThreadId;

/// Add a biunion's four phases to `graph`, reading `inputs` and draining into `output`. Its work
/// and output slots are allocated here, after any parents of `inputs` were built.
fn assemble<
    'params,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
    LeftTargetType,
    RightTargetType,
    OutputType,
>(
    factory: FactoryType,
    inputs: Inputs<Ready<LeftTargetType>, Ready<RightTargetType>>,
    output: OutputType,
    budget: Budget,
    mut graph: Graph<'params, ThreadIdType>,
) -> Graph<'params, ThreadIdType>
where
    Left: 'static,
    Right: 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
    LeftTargetType: Receivable<DataType = Left, SignalType = SignalType> + 'params,
    RightTargetType: Receivable<DataType = Right, SignalType = SignalType> + 'params,
    OutputType: Rotate<DataType = Out, SignalType = SignalType> + 'params,
{
    let work = graph.allocator.next();
    let output_slot = graph.allocator.next();
    let left_waker = graph.allocator.local_waker(inputs.left.id);
    let right_waker = graph.allocator.local_waker(inputs.right.id);
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
    graph
}

// ============================================================
// Both Sync inputs
// ============================================================

/// Sync, Sync → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'params, Sync, Sync, Sync, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
where
    Left: Send + std::marker::Sync + 'static,
    Right: Send + std::marker::Sync + 'static,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        Ok(assemble(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        ))
    }
}

/// Sync, Sync → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'params, Sync, Sync, Deferred, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
where
    Left: Send + std::marker::Sync + 'static,
    Right: Send + std::marker::Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(self.output);
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}

/// Sync, Sync → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<'params, Sync, Sync, Deferred, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
where
    Left: Send + std::marker::Sync + 'static,
    Right: Send + std::marker::Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    type OutType = Out;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<Out, SignalType>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(edge);
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}

// ============================================================
// Async left, Sync right
// ============================================================

/// Async, Sync → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'params, Async, Sync, Sync, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
where
    Left: 'static,
    Right: Send + std::marker::Sync + 'static,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        Ok(assemble(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        ))
    }
}

/// Async, Sync → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Async,
        Sync,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    Left: 'static,
    Right: Send + std::marker::Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(self.output);
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}

/// Async, Sync → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'params,
        Async,
        Sync,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    Left: 'static,
    Right: Send + std::marker::Sync + 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    type OutType = Out;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<Out, SignalType>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let (left, graph) =
            self.input
                .left
                .ready(self.alloc.left, self.options.bound.left, graph)?;
        let right = self.input.right.ready();
        let inputs = Inputs { left, right };
        let output = Single::from(edge);
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}

// ============================================================
// Sync left, Async right
// ============================================================

/// Sync, Async → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'params, Sync, Async, Sync, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
where
    Left: Send + std::marker::Sync + 'static,
    Right: 'static,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        Ok(assemble(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        ))
    }
}

/// Sync, Async → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Sync,
        Async,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    Left: Send + std::marker::Sync + 'static,
    Right: 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        let output = Single::from(self.output);
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}

/// Sync, Async → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'params,
        Sync,
        Async,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    Left: Send + std::marker::Sync + 'static,
    Right: 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    type OutType = Out;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<Out, SignalType>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let graph = Graph::empty(allocator);
        let left = self.input.left.ready();
        let (right, graph) =
            self.input
                .right
                .ready(self.alloc.right, self.options.bound.right, graph)?;
        let inputs = Inputs { left, right };
        let output = Single::from(edge);
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}

// ============================================================
// Both Async inputs
// ============================================================

/// Async, Async → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<'params, Async, Async, Sync, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
where
    Left: 'static,
    Right: 'static,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
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
        Ok(assemble(
            self.factory,
            inputs,
            self.output,
            self.options.budget,
            graph,
        ))
    }
}

/// Async, Async → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Async,
        Async,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    Left: 'static,
    Right: 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
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
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}

/// Async, Async → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'params,
        Async,
        Async,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    Left: 'static,
    Right: 'static,
    Out: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    type OutType = Out;
    type SignalType = SignalType;
    type ThreadIdType = ThreadIdType;

    fn build(
        self: Box<Self>,
        edge: edge::Sender<Out, SignalType>,
        allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
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
        Ok(assemble(
            self.factory,
            inputs,
            output,
            self.options.budget,
            graph,
        ))
    }
}
