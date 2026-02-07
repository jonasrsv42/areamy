use crate::node::line::pull::node::Line;
use crate::{LineRoutine, Origin, ThreadId};
use crate::{Pullable, Trackable};
use std::marker::PhantomData;

/// [`make_pull`] creates a [Pullable] connection. The child takes ownership
/// of the parent. This connection does not use synchronization or dynamic dispatch
/// and is well suited for line segments in the graph where performance is necessary
/// and where synchronization or message passing could be a overhead.
pub fn make_pull<In, Out, SignalType, ThreadIdType, RoutineType, PullableType>(
    pullable: PullableType,
    worker: RoutineType,
) -> Line<In, Out, SignalType, ThreadIdType, RoutineType, PullableType>
where
    ThreadIdType: ThreadId + 'static,
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    RoutineType: 'static + LineRoutine<In, Out>,
    PullableType:
        Pullable<ThreadId = ThreadIdType, DataType = In, SignalType = SignalType> + 'static,
{
    Line::new(worker, pullable)
}

/// [`Connect`] is a [Pullable] version of [crate::work::Connect], it's a wrapper around [make_pull] for type hints.
pub struct Connect<DataType, SignalType = Trackable<&'static str>>
where
    DataType: Send + Sync,
    SignalType: Origin,
{
    datatype: PhantomData<DataType>,
    signaltype: PhantomData<SignalType>,
}

impl<DataType, SignalType> Connect<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    /// [Connect::pull] is the same as [make_pull] but with type hints.
    pub fn pull<Out, ThreadIdType, RoutineType, PullableType>(
        pullable: PullableType,
        worker: RoutineType,
    ) -> Line<DataType, Out, SignalType, ThreadIdType, RoutineType, PullableType>
    where
        ThreadIdType: ThreadId + 'static,
        Out: Send + Sync + 'static,
        RoutineType: 'static + LineRoutine<DataType, Out>,
        PullableType: Pullable<ThreadId = ThreadIdType, DataType = DataType, SignalType = SignalType>
            + 'static,
    {
        make_pull(pullable, worker)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::routine::tests::MockLine;
    use crate::pull::Sink;
    use crate::source::pull::SourceBuffer;
    use crate::work::Source;
    use crate::{DefaultThread, Message, Pullable, Pushable};

    #[test]
    fn line_builder_reading_source_buffer() {
        let mut buffer = SourceBuffer::<usize, usize, DefaultThread>::new();
        let mut source = Source::of(&buffer).unwrap();

        source.push(Message::Data(5)).unwrap();
        source.push(Message::Data(6)).unwrap();

        assert_eq!(buffer.pull().unwrap(), Message::Data(5));
        assert_eq!(buffer.pull().unwrap(), Message::Data(6));
    }

    #[test]
    fn line_builder_make_pull() {
        let buffer = SourceBuffer::<usize, usize, DefaultThread>::new();
        let mut source = Source::of(&buffer).unwrap();

        let line = make_pull(buffer, MockLine::new());
        let mut sink = Sink::new(line);

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.pull().unwrap(), Message::Data(2));
        assert_eq!(sink.pull().unwrap(), Message::Data(6));
    }
}
