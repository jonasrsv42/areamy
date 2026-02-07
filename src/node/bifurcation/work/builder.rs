use crate::{
    BifurcationRoutine, Origin, ThreadId, Workable, graph::Add,
    node::bifurcation::work::node::BifurcationTrait, work::Bifurcation,
};

pub fn make_bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>(
    worker: RoutineType,
) -> Box<
    impl BifurcationTrait<
        In = In,
        Left = Left,
        Right = Right,
        Signal = SignalType,
        BifurcationRoutine = RoutineType,
    > + Add<dyn Workable<ThreadId = ThreadIdType>>
    + Workable<ThreadId = ThreadIdType>,
>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync + 'static,
    Right: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: 'static + BifurcationRoutine<In, Left, Right>,
{
    Box::new(Bifurcation::of(worker))
}
