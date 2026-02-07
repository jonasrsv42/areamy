//! Bridge a [LineTrait] with a [crate::pull::Line]

use crate::node::line::work::node::LineTrait;
use crate::{LineRoutine, Origin, Pullable, ThreadId, Workable, graph::Add, work::Line};
use crate::{Pushable, marker::Connection};

/// [`Bridge`] is a bridge between a [Pullable] and [Workable] segment.
/// it holds a [Pullable] type which it'll [Pullable::pull] when scheduled
/// and it will [Pushable::push] data into its [Bridge::pushable].
///
/// [Bridge] itself implements [Workable] so that child nodes can treat
/// it as part of a [Workable] graph segment.
pub struct Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType:
        Pushable<DataType = PullableType::DataType, SignalType = PullableType::SignalType>,
{
    /// Parent [Pullable] edge.
    pub pullable: PullableType,
    /// Child [Pushable] edge.
    pub pushable: PushableType,
}

impl<PullableType, PushableType> Connection for Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType:
        Pushable<DataType = PullableType::DataType, SignalType = PullableType::SignalType>,
{
}

impl<PullableType, PushableType> Workable for Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType:
        Pushable<DataType = PullableType::DataType, SignalType = PullableType::SignalType>,
{
    type ThreadId = PullableType::ThreadId;

    /// Upon [Bridge::work] we pull our parent ([Bridge::pullable]) and [Pushable::push] into the
    /// child ([Bridge::pushable])
    fn work(&mut self) -> Result<(), crate::error::Error> {
        let msg = self.pullable.pull()?;
        self.pushable.push(msg)
    }
}

impl<PullableType, PushableType> Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType:
        Pushable<DataType = PullableType::DataType, SignalType = PullableType::SignalType>,
{
    /// Create a [Bridge] from a [Pullable] and [Pushable].
    pub fn new(pullable: PullableType, pushable: PushableType) -> Self {
        Self { pullable, pushable }
    }
}

// Connect a synchronous worker with a non-synchronouys pullable to create synchronous
// node.
/// [bridge_nosync] creates a [LineTrait] implementation from a [LineRoutine] and
/// a [Pullable]. The [LineTrait] can the be further connected to other graph nodes using
/// the same functions as documented in [crate::work::make_line].
///
/// * `pullable` - a [Pullable] type that will be owned by [LineTrait]
/// * `worker` - a [LineRoutine] that will be in the resulting [LineTrait] node.
pub fn bridge_nosync<In, Out, SignalType, ThreadIdType, RoutineType, PullableType>(
    pullable: PullableType,
    worker: RoutineType,
) -> Box<
    impl LineTrait<In = In, Out = Out, Signal = SignalType, LineRoutine = RoutineType>
    + Workable<ThreadId = ThreadIdType>
    + Add<dyn Workable<ThreadId = ThreadIdType>>
    + Send,
>
where
    ThreadIdType: ThreadId + 'static,
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    RoutineType: 'static + LineRoutine<In, Out>,
    PullableType:
        Pullable<ThreadId = ThreadIdType, DataType = In, SignalType = SignalType> + 'static,
{
    let mut line = Line::of(worker);
    let bridge = Bridge::new(pullable, line.input.clone());
    line.workers.push(Box::new(bridge));

    Box::new(line)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::routine::tests::MockLine;
    use crate::{Message, work::Sink, work::Source};

    #[test]
    fn line_basic_bridge() {
        let buffer = crate::pull::SourceBuffer::new();
        let mut source = Source::new(&buffer).unwrap();

        let line = bridge_nosync(buffer, MockLine::new());
        let mut sink = Sink::new(line).unwrap();

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Data(6));

        source.push(Message::Flush("hi".into())).unwrap();
        assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(4));
    }
}
