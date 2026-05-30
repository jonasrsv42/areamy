//! GraphBuilder and AsyncParent impls for all biunion edge combinations.

use super::node::{Allocated, Node};
use crate::ThreadId;
use crate::connect::poll::edge::{Async, Deferred, PollEdge, Sync};
use crate::connect::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::connect::poll::traits::AsyncParent;
use crate::connect::poll::wakers::ThreadLocalWakerAllocator;
use crate::connect::waker::ThreadLocalWaker;
use crate::error::Error;
use crate::node::biunion::poll::factory::{BiunionInputs, BiunionRoutineFactory, BiunionWakers};
use crate::node::biunion::poll::node::{self, InputPhases, Phase};
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;
use std::cell::RefCell;
use std::rc::Rc;

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
    edge_waker: ThreadLocalWaker,
    mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
) -> Result<
    (
        Vec<Rc<RefCell<PollEdge<DataType, SignalType>>>>,
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
        let edge = Rc::new(RefCell::new(PollEdge::new(edge_waker.clone())));
        let parent_graph = parent.build(edge.clone(), allocator)?;
        allocator = parent_graph.allocator;
        nodes.extend(parent_graph.nodes);
        edges.push(edge);
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
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
            left_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_id = self.input.right.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
            left_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_id = self.input.right.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
        edge: Rc<RefCell<PollEdge<Out, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_id = self.input.right.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: edge,
                waker: output.value.local,
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
            right_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let left_id = self.input.left.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
            right_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let left_id = self.input.left.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
        edge: Rc<RefCell<PollEdge<Out, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let right_slot = allocator.next();
        let (right_edges, mut nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let left_id = self.input.left.slot.id;
        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: edge,
                waker: output.value.local,
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
            left_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_slot = allocator.next();
        let (right_edges, right_nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;
        nodes.extend(right_nodes);

        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
            left_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_slot = allocator.next();
        let (right_edges, right_nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;
        nodes.extend(right_nodes);

        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: self.output,
                waker: output.value.local,
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
        edge: Rc<RefCell<PollEdge<Out, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();
        let left_id = self.input.left.slot.id;
        let right_id = self.input.right.slot.id;
        let left_waker = allocator.local_waker(left_id);
        let right_waker = allocator.local_waker(right_id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: edge,
                waker: output.value.local,
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
        edge: Rc<RefCell<PollEdge<Out, SignalType>>>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<'params, ThreadIdType>, Error> {
        let left_slot = allocator.next();
        let (left_edges, mut nodes, returned_allocator) = build_parents(
            self.input.left.parents,
            left_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;

        let right_slot = allocator.next();
        let (right_edges, right_nodes, returned_allocator) = build_parents(
            self.input.right.parents,
            right_slot.value.local.clone(),
            allocator,
        )?;
        allocator = returned_allocator;
        nodes.extend(right_nodes);

        let work = allocator.next();
        let output = allocator.next();
        let left_waker = allocator.local_waker(left_slot.id);
        let right_waker = allocator.local_waker(right_slot.id);
        let routine = self.factory.create(BiunionWakers {
            input: BiunionInputs {
                left: left_waker.clone(),
                right: right_waker.clone(),
            },
            work: work.value.local.clone(),
            output: output.value.local.clone(),
        });
        let phases = node::new_phases(
            InputPhases {
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
                waker: work.value.local,
            },
            Phase {
                target: edge,
                waker: output.value.local,
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
