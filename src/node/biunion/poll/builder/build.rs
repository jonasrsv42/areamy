//! GraphBuilder and AsyncParent impls for all biunion edge combinations.

use super::node::{Allocated, Node};
use crate::error::Error;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::factory::{BiunionRoutineFactory, BiunionWakers};
use crate::node::biunion::poll::node;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::Phase;
use crate::poll::edge::{self, Async, Deferred, Receiver, Sync};
use crate::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::poll::traits::AsyncParent;
use crate::poll::wakers::ThreadLocalWakerAllocator;
use crate::signal::Origin;
use crate::thread::ThreadId;
use crate::wake::LocalWaker;

/// Build async parent edges, returning edges + collected nodes + allocator.
fn build_parents<'params, DataType, SignalType, ThreadIdType>(
    parents: Vec<
        Box<
            dyn AsyncParent<
                    'params,
                    OutType = DataType,
                    SignalType = SignalType,
                    ThreadIdType = ThreadIdType,
                > + 'params,
        >,
    >,
    edge_waker: LocalWaker,
    mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
) -> Result<
    (
        Vec<Receiver<DataType, SignalType>>,
        Vec<GraphNode<'params, ThreadIdType>>,
        ThreadLocalWakerAllocator<ThreadIdType>,
    ),
    Error,
>
where
    DataType: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
{
    let mut edges = Vec::new();
    let mut nodes = Vec::new();
    for parent in parents {
        let (sender, receiver) = edge::channel(edge_waker.clone(), None);
        let parent_graph = parent.build(sender, allocator)?;
        allocator = parent_graph.allocator;
        nodes.extend(parent_graph.nodes);
        edges.push(receiver);
    }
    Ok((edges, nodes, allocator))
}

// ============================================================
// Both Sync inputs
// ============================================================

/// Sync, Sync → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
        Sync,
        Sync,
        Sync,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let left_id = self.input.left.slot.id;
        let right_id = self.input.right.slot.id;
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: self.input.left.edge,
                    waker: left_waker,
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        let nodes = vec![
            GraphNode {
                id: left_id,
                pollable: Box::new(phases.inputs.left),
            },
            GraphNode {
                id: right_id,
                pollable: Box::new(phases.inputs.right),
            },
            GraphNode {
                id: work.id,
                pollable: Box::new(phases.work),
            },
            GraphNode {
                id: output.id,
                pollable: Box::new(phases.output),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Sync, Sync → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
        Sync,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let left_id = self.input.left.slot.id;
        let right_id = self.input.right.slot.id;
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: self.input.left.edge,
                    waker: left_waker,
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        let nodes = vec![
            GraphNode {
                id: left_id,
                pollable: Box::new(phases.inputs.left),
            },
            GraphNode {
                id: right_id,
                pollable: Box::new(phases.inputs.right),
            },
            GraphNode {
                id: work.id,
                pollable: Box::new(phases.work),
            },
            GraphNode {
                id: output.id,
                pollable: Box::new(phases.output),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

// ============================================================
// Async left, Sync right
// ============================================================

/// Async, Sync → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
        Async,
        Sync,
        Sync,
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
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_id = self.input.right.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: left_edges,
                    waker: left_waker,
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_slot.id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Async, Sync → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_id = self.input.right.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: left_edges,
                    waker: left_waker,
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_slot.id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Async, Sync → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'params,
        Allocated,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_id = self.input.right.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: left_edges,
                    waker: left_waker,
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: edge,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_slot.id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

// ============================================================
// Sync left, Async right
// ============================================================

/// Sync, Async → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
        Sync,
        Async,
        Sync,
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
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let right_slot = allocator.next();
        let (right_edges, mut nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let left_id = self.input.left.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: self.input.left.edge,
                    waker: left_waker,
                },
                right: Phase {
                    target: right_edges,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_slot.id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Sync, Async → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let right_slot = allocator.next();
        let (right_edges, mut nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let left_id = self.input.left.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: self.input.left.edge,
                    waker: left_waker,
                },
                right: Phase {
                    target: right_edges,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_slot.id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Sync, Async → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'params,
        Allocated,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let right_slot = allocator.next();
        let (right_edges, mut nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let left_id = self.input.left.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: self.input.left.edge,
                    waker: left_waker,
                },
                right: Phase {
                    target: right_edges,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: edge,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_slot.id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

// ============================================================
// Both Async inputs
// ============================================================

/// Async, Async → Sync output
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
        Async,
        Async,
        Sync,
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
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_slot = allocator.next();
        let (right_edges, right_nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;
        nodes.extend(right_nodes);

        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: left_edges,
                    waker: left_waker,
                },
                right: Phase {
                    target: right_edges,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_slot.id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_slot.id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

/// Async, Async → Deferred output (sink)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    GraphBuilder<'params, ThreadIdType>
    for Node<
        'params,
        Allocated,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_slot = allocator.next();
        let (right_edges, right_nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;
        nodes.extend(right_nodes);

        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: left_edges,
                    waker: left_waker,
                },
                right: Phase {
                    target: right_edges,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: self.output,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_slot.id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_slot.id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}

// ============================================================
// AsyncParent — biunion consumed by downstream .parent()
// ============================================================

/// Sync, Sync → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'params,
        Allocated,
        Sync,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let left_id = self.input.left.slot.id;
        let right_id = self.input.right.slot.id;
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: self.input.left.edge,
                    waker: left_waker,
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: edge,
                waker: output.value.wake.local,
            },
        );
        let nodes = vec![
            GraphNode {
                id: left_id,
                pollable: Box::new(phases.inputs.left),
            },
            GraphNode {
                id: right_id,
                pollable: Box::new(phases.inputs.right),
            },
            GraphNode {
                id: work.id,
                pollable: Box::new(phases.work),
            },
            GraphNode {
                id: output.id,
                pollable: Box::new(phases.output),
            },
        ];
        Ok(Graph { allocator, nodes })
    }
}

/// Async, Async → Deferred output (consumed as parent)
impl<'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType> AsyncParent<'params>
    for Node<
        'params,
        Allocated,
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
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_slot = allocator.next();
        let (right_edges, right_nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.wake.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;
        nodes.extend(right_nodes);

        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: Inputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.wake.local.clone(),
            timer: work.value.timer.clone(),
            output: output.value.wake.local.clone(),
        });
        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: left_edges,
                    waker: left_waker,
                },
                right: Phase {
                    target: right_edges,
                    waker: right_waker,
                },
            },
            Phase {
                target: routine,
                waker: work.value.wake.local,
            },
            Phase {
                target: edge,
                waker: output.value.wake.local,
            },
        );
        nodes.push(GraphNode {
            id: left_slot.id,
            pollable: Box::new(phases.inputs.left),
        });
        nodes.push(GraphNode {
            id: right_slot.id,
            pollable: Box::new(phases.inputs.right),
        });
        nodes.push(GraphNode {
            id: work.id,
            pollable: Box::new(phases.work),
        });
        nodes.push(GraphNode {
            id: output.id,
            pollable: Box::new(phases.output),
        });
        Ok(Graph { allocator, nodes })
    }
}
