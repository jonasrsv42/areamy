use crate::error::Error;
use crate::node::line::sync::bridge::Bridge;
use crate::node::line::sync::node::LineTrait;
use crate::{graph::Add, sync::Line, LineRoutine, Message, Origin, Pullable, ThreadId, Workable};

pub fn make_line<In, Out, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    Box<
        impl LineTrait<In = In, Out = Out, Signal = SignalType, LineRoutine = RoutineType>
            // Need to specify workables outside of the `LineTrait` to disambiguate.
            + Workable<ThreadId = ThreadIdType>
            + Add<dyn Workable<ThreadId = ThreadIdType>>
            + Send,
    >,
    Error,
>
where
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + Clone,
    RoutineType: LineRoutine<In, Out> + 'static,
{
    let worker = maybe_worker?;

    Ok(Box::new(Line::<
        In,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
    >::of(worker)))
}

// Connect a synchronous worker with a non-synchronouys pullable to create synchronous
// node.
pub fn bridge_nosync<In, Out, SignalType, ThreadIdType, RoutineType, PullableType>(
    pullable: PullableType,
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    Box<
        impl LineTrait<In = In, Out = Out, Signal = SignalType, LineRoutine = RoutineType>
            + Workable<ThreadId = ThreadIdType>
            + Add<dyn Workable<ThreadId = ThreadIdType>>
            + Send,
    >,
    Error,
>
where
    ThreadIdType: ThreadId + Clone + 'static,
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    RoutineType: 'static + LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, Message = Message<In, SignalType>> + 'static,
{
    let worker = maybe_worker?;
    let mut line = Line::of(worker);
    let bridge = Bridge::new(pullable, line.input.clone());
    line.workers.push(Box::new(bridge));

    Ok(Box::new(line))
}
