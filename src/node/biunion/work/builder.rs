use crate::Origin;
use crate::ThreadId;
use crate::node::biunion::{BiunionRoutine, work::Biunion};

pub fn make_biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>(
    worker: RoutineType,
) -> Box<Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>>
where
    ThreadIdType: ThreadId,
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    RoutineType: 'params + BiunionRoutine<Left, Right, Out>,
{
    Box::new(Biunion::<
        'params,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
    >::of(worker))
}
