use crate::error::Error;
use crate::node::biunion::{
    sync::node::BiunionTrait, sync::node::LeftSource, sync::node::RightSource, sync::Biunion,
    BiunionRoutine,
};
use crate::ThreadId;
use crate::{graph::Add, Origin, Workable};

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
            + Add<dyn Workable<ThreadId = ThreadIdType>, LeftSource>
            + Add<dyn Workable<ThreadId = ThreadIdType>, RightSource>,
    >,
    Error,
>
where
    ThreadIdType: ThreadId + 'static,
    Left: Clone + Send + Sync + 'static,
    Right: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    RoutineType: 'static + BiunionRoutine<Left, Right, Out>,
{
    let worker = maybe_worker?;

    Ok(Box::new(Biunion::of(worker)))
}
