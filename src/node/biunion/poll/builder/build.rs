//! GraphBuilder and AsyncParent impls for all biunion edge combinations.

use super::node::{Allocated, Node};
use crate::ThreadId;
use crate::connect::poll::edge::{Deferred, Sync};
use crate::connect::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::connect::poll::wakers::ThreadLocalWakerAllocator;
use crate::error::Error;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::node::{self, InputPhases, Phase};
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;

// ============================================================
// Both Sync inputs
// ============================================================

/// Sync, Sync → Sync output
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType> GraphBuilder<ThreadIdType>
    for Node<Allocated, Sync, Sync, Sync, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
where
    Left: Send + std::marker::Sync + 'static,
    Right: Send + std::marker::Sync + 'static,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    FactoryType: BiunionRoutineFactory + 'static,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();

        let left_id = self.input.left.slot.id;
        let right_id = self.input.right.slot.id;
        let routine = self.factory.create(output.value.local.clone());
        let phases = node::new_phases(
            InputPhases {
                left: Phase {
                    target: self.input.left.edge,
                    waker: allocator.local_waker(left_id),
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: allocator.local_waker(right_id),
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
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType> GraphBuilder<ThreadIdType>
    for Node<
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
    ThreadIdType: ThreadId + 'static,
    FactoryType: BiunionRoutineFactory + 'static,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out> + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<ThreadIdType>,
    ) -> Result<Graph<ThreadIdType>, Error> {
        let work = allocator.next();
        let output = allocator.next();

        let left_id = self.input.left.slot.id;
        let right_id = self.input.right.slot.id;
        let routine = self.factory.create(output.value.local.clone());
        let phases = node::new_phases(
            InputPhases {
                left: Phase {
                    target: self.input.left.edge,
                    waker: allocator.local_waker(left_id),
                },
                right: Phase {
                    target: self.input.right.edge,
                    waker: allocator.local_waker(right_id),
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
