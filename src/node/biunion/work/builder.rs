use crate::ThreadId;
use crate::biunion;
use crate::error::Error;
use crate::node::biunion::{BiunionRoutine, work::Biunion, work::node::BiunionTrait};
use crate::{Origin, Workable, graph::Add};

pub fn make_biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    Box<
        impl BiunionTrait<
            Left = Left,
            Right = Right,
            Out = Out,
            Signal = SignalType,
            BiunionRoutine = RoutineType,
        > + Workable<ThreadId = ThreadIdType>
        + Add<dyn Workable<ThreadId = ThreadIdType>, biunion::Left>
        + Add<dyn Workable<ThreadId = ThreadIdType>, biunion::Right>,
    >,
    Error,
>
where
    ThreadIdType: ThreadId + 'static,
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    RoutineType: 'static + BiunionRoutine<Left, Right, Out>,
{
    let worker = maybe_worker?;

    Ok(Box::new(Biunion::of(worker)))
}
