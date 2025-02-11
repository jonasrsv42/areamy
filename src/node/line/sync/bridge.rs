//! Bridge a [LineTrait] with a [crate::nosync::Line]

use crate::error::Error;
use crate::node::line::sync::node::LineTrait;
use crate::{graph::Add, sync::Line, LineRoutine, Message, Origin, Pullable, ThreadId, Workable};
use crate::{marker::Connection, Pushable};

/// [`Bridge`] is a bridge between a [Pullable] and [Workable] segment.
/// it holds a [Pullable] type which it'll [Pullable::pull] when scheduled
/// and it will [Pushable::push] data into its [Bridge::pushable].
///
/// [Bridge] itself implements [Workable] so that child nodes can treat
/// it as part of a [Workable] graph segment.
pub struct Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType: Pushable<Message = PullableType::Message>,
{
    /// Parent [Pullable] edge.
    pub pullable: PullableType,
    /// Child [Pushable] edge.
    pub pushable: PushableType,
}

impl<PullableType, PushableType> Connection for Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType: Pushable<Message = PullableType::Message>,
{
}

impl<PullableType, PushableType> Workable for Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType: Pushable<Message = PullableType::Message>,
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
    PushableType: Pushable<Message = PullableType::Message>,
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
/// the same functions as documented in [crate::sync::make_line].
///
/// * `pullable` - a [Pullable] type that will be owned by [LineTrait]
/// * `maybe_worker` - a [LineRoutine] that will be in the resulting [LineTrait] node.
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::routine::tests::MockLine;
    use crate::{sync::Sink, sync::Source};

    #[test]
    fn line_basic_bridge() {
        let root = crate::nosync::Root::new();
        let mut source = Source::new(&root).unwrap();

        let line = bridge_nosync(root, MockLine::new()).unwrap();
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
