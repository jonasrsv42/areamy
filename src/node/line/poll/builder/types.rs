//! What a poll line builder carries besides its edge kinds.
//!
//! The edge kinds stay direct parameters of [`Node`](super::node::Node): impls pick by them.
//! The rest ride along unchanged through every transition, so they are one parameter.

use crate::node::line::poll::factory::LineRoutineFactory;
use crate::node::line::poll::routine::LineRoutine;
use crate::signal::Origin;
use crate::thread::ThreadId;
use core::marker::PhantomData;

/// A poll line's data, signal, thread and routine factory. Bounds every build needs live here.
pub trait Types<'params>: 'params {
    type In: 'static;
    type Out: 'static;
    type Signal: Origin + Clone + Send + core::marker::Sync + 'static;
    type ThreadId: ThreadId;
    type Factory: LineRoutineFactory<'params, Routine: LineRoutine<Self::In, Self::Out>>;
}

/// [Types] from its parts, so `thread.line(factory)` infers them.
pub struct PollLine<In, Out, Signal, ThreadIdType, Factory>(
    PhantomData<fn() -> (In, Out, Signal, ThreadIdType, Factory)>,
);

impl<'params, In, Out, Signal, ThreadIdType, Factory> Types<'params>
    for PollLine<In, Out, Signal, ThreadIdType, Factory>
where
    In: 'static,
    Out: 'static,
    Signal: Origin + Clone + Send + core::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    Factory: LineRoutineFactory<'params>,
    Factory::Routine: LineRoutine<In, Out>,
{
    type In = In;
    type Out = Out;
    type Signal = Signal;
    type ThreadId = ThreadIdType;
    type Factory = Factory;
}
