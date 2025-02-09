use crate::error::Error;
use crate::{
    graph::Add, node::bifurcation::sync::node::BifurcationTrait, sync::Bifurcation,
    BifurcationRoutine, Origin, ThreadId, Workable,
};

pub fn make_bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    Box<
        impl BifurcationTrait<
                In = In,
                Left = Left,
                Right = Right,
                Signal = SignalType,
                BifurcationRoutine = RoutineType,
            > + Add<dyn Workable<ThreadId = ThreadIdType>>
            + Workable<ThreadId = ThreadIdType>,
    >,
    Error,
>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync + 'static,
    Right: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: 'static + BifurcationRoutine<In, Left, Right>,
{
    let worker = maybe_worker?;

    Ok(Box::new(Bifurcation::of(worker)))
}
