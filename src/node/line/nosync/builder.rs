use crate::error::Error;
use crate::node::line::nosync::node::Line;
use crate::{LineRoutine, Message, Origin};
use crate::{NoPull, Pullable, ThreadId, Trackable};
use std::marker::PhantomData;

// Root of a nosync line. It will have to have input
// pushed into it from elsewhere.
//
// Root of the `nosync` line. Is just a nosync line node
// that has no `pullable` input.
//
// Node 1 -> Node 2 -> Node 3
//
// Node 1 is root because it cannot pull from anywhere
// and instead awaits external input.
pub fn root<In, Out, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    Line<
        In,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        NoPull<ThreadIdType, Message<In, SignalType>>,
    >,
    Error,
>
where
    ThreadIdType: ThreadId + 'static,
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    RoutineType: 'static + LineRoutine<In, Out>,
{
    let worker = maybe_worker?;

    Ok(Line::root(worker))
}

// Connect a nosync node to another one.
//
// This is a pullable connection.
pub fn make_pull<In, Out, SignalType, ThreadIdType, RoutineType, PullableType>(
    pullable: PullableType,
    maybe_worker: Result<RoutineType, Error>,
) -> Result<Line<In, Out, SignalType, ThreadIdType, RoutineType, PullableType>, Error>
where
    ThreadIdType: ThreadId + 'static,
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    RoutineType: 'static + LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, Message = Message<In, SignalType>> + 'static,
{
    let worker = maybe_worker?;

    Ok(Line::new(worker, Some(pullable)))
}

// Utility function for typed graph declarations. These functions are only useful for improving
// readability and are not needed for anything else. The `make_pull` function suffice for building
// graphs because types can be inferred from context.
pub struct Connect<DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync + Clone,
    SignalType: Origin,
{
    datatype: PhantomData<DataType>,
    signaltype: PhantomData<SignalType>,
}

impl<DataType, SignalType> Connect<DataType, SignalType>
where
    DataType: Send + Sync + Clone + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
{
    pub fn pull<Out, ThreadIdType, RoutineType, PullableType>(
        pullable: PullableType,
        maybe_worker: Result<RoutineType, Error>,
    ) -> Result<Line<DataType, Out, SignalType, ThreadIdType, RoutineType, PullableType>, Error>
    where
        ThreadIdType: ThreadId + 'static,
        Out: Clone + Send + Sync + 'static,
        RoutineType: 'static + LineRoutine<DataType, Out>,
        PullableType:
            Pullable<ThreadId = ThreadIdType, Message = Message<DataType, SignalType>> + 'static,
    {
        make_pull(pullable, maybe_worker)
    }
}
