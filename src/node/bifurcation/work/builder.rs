use crate::node::bifurcation::routine::BifurcationRoutine;
use crate::node::bifurcation::work::node::Bifurcation;
use crate::signal::Origin;
use crate::thread::ThreadId;

pub fn make_bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>(
    worker: RoutineType,
) -> Box<Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync + 'static,
    Right: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: 'params + BifurcationRoutine<In, Left, Right>,
{
    Box::new(Bifurcation::<
        'params,
        In,
        Left,
        Right,
        SignalType,
        ThreadIdType,
        RoutineType,
    >::of(worker))
}
