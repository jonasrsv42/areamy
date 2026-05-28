//! Bridge a [LineTrait] with a [crate::pull::Line]

use crate::{LineRoutine, Origin, Pullable, ThreadId, Workable, work::Line};
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
        Pushable<DataType = PullableType::DataType, SignalType = PullableType::SignalType> + Send,
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

/// Build a [`crate::work::Line`] node fed by a [Pullable] parent.
///
/// Bridges a pull-graph segment (the `pullable`) into a work-graph
/// node (built from `worker`). The resulting node can be wired to
/// other work-graph nodes via [crate::make_bidi], [crate::make_push]
/// and [crate::make_work] like any other line node.
///
/// Returns a concrete `Box<Line<...>>` rather than `Box<impl LineTrait + ...>`
/// — `impl Trait` returns hide type info from the inferencer in cyclic
/// graphs, matching the policy used by [`crate::work::make_line`].
///
/// * `pullable` - a [Pullable] parent owned by the returned node.
/// * `worker` - the [LineRoutine] that processes data inside the node.
pub fn from_pull<'params, In, Out, SignalType, ThreadIdType, RoutineType, PullableType>(
    pullable: PullableType,
    worker: RoutineType,
) -> Box<Line<'params, In, Out, SignalType, ThreadIdType, RoutineType>>
where
    ThreadIdType: ThreadId,
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    RoutineType: 'params + LineRoutine<In, Out>,
    PullableType:
        Pullable<ThreadId = ThreadIdType, DataType = In, SignalType = SignalType> + Send + 'params,
{
    let mut line = Line::<'params, In, Out, SignalType, ThreadIdType, RoutineType>::of(worker);
    let bridge = Bridge::new(pullable, line.input.sender());
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

        let line = from_pull(buffer, MockLine::new());
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
