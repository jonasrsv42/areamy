//! What a poll biunion builder carries besides its edge kinds.
//!
//! The edge kinds stay direct parameters of [`Node`](super::node::Node): impls pick by them.
//! The rest ride along unchanged through every transition, so they are one parameter.

use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

/// A poll biunion's data, signal, thread and routine factory. Bounds every build needs live here.
pub trait Types<'params>: 'params {
    type Left: 'static;
    type Right: 'static;
    type Out: 'static;
    type Signal: Origin + Clone + Send + std::marker::Sync + 'static;
    type ThreadId: ThreadId;
    type Factory: BiunionRoutineFactory<'params, Routine: BiunionRoutine<Self::Left, Self::Right, Self::Out>>;
}

/// [Types] from its parts, so `thread.biunion(factory)` infers them.
pub struct PollBiunion<Left, Right, Out, Signal, ThreadIdType, Factory>(
    PhantomData<fn() -> (Left, Right, Out, Signal, ThreadIdType, Factory)>,
);

impl<'params, Left, Right, Out, Signal, ThreadIdType, Factory> Types<'params>
    for PollBiunion<Left, Right, Out, Signal, ThreadIdType, Factory>
where
    Left: 'static,
    Right: 'static,
    Out: 'static,
    Signal: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    Factory: BiunionRoutineFactory<'params>,
    Factory::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Left = Left;
    type Right = Right;
    type Out = Out;
    type Signal = Signal;
    type ThreadId = ThreadIdType;
    type Factory = Factory;
}
