use crate::error::Error;
use crate::node::line::nosync::node::Line;
use crate::SyncQueue;
use crate::{graph::Get, Connection, Pullable, Pushable, ThreadId, Trackable};
use crate::{LineRoutine, Message, Origin};
use std::marker::PhantomData;
use std::sync::Arc;

/// [`Root`] of a [`Pullable`] subgraph. It will await input into its [Pushable] and
/// then forward it downstream.
pub struct Root<MessageType, ThreadIdType>
where
    MessageType: Clone,
    ThreadIdType: ThreadId,
{
    /// The input queue to the [Pullable] chain. For now we have a [SyncQueue]
    /// but will support [std::collections::VecDeque] in the future as well.
    ///
    /// TODO: support having a sync or nosync input.
    pub input: Arc<SyncQueue<MessageType>>,

    /// The threadId that belongs to the child nodes.
    thread: PhantomData<ThreadIdType>,
}

impl<MessageType, ThreadIdType> Root<MessageType, ThreadIdType>
where
    MessageType: Clone,
    ThreadIdType: ThreadId,
{
    pub fn new() -> Self {
        Self {
            input: Arc::new(SyncQueue::new()),
            thread: PhantomData,
        }
    }
}

/// [Root] is a [Pullable] [Connection] in our graph.
impl<MessageType: Clone, ThreadIdType: ThreadId> Connection for Root<MessageType, ThreadIdType> {}

/// [Get] the [Pushable] from [Root].
impl<MessageType, ThreadIdType> Get<dyn Pushable<Message = MessageType>>
    for Root<MessageType, ThreadIdType>
where
    MessageType: Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    fn get(&self) -> Result<Box<dyn Pushable<Message = MessageType>>, Error> {
        Get::get(&self.input)
    }
}

/// [Root] is [Pullable] it will serve as the root node of a [Pullable] subgraph.
impl<MessageType, ThreadIdType> Pullable for Root<MessageType, ThreadIdType>
where
    MessageType: Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    type ThreadId = ThreadIdType;
    type Message = MessageType;

    fn pull(&mut self) -> Result<Self::Message, Error> {
        self.input.read_front()
    }
}

/// [`make_pull`] creates a [Pullable] connection. The child takes ownership
/// of the parent. This connection does not use synchronization or dynamic dispatch
/// and is well suited for line segments in the graph where performance is necessary
/// and where synchronization or message passing could be a overhead.
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

    Ok(Line::new(worker, pullable))
}

/// [`Connect`] is a [Pullable] version of [crate::sync::Connect], it's a wrapper around [make_pull] for type hints.
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
    /// [Connect::pull] is the same as [make_pull] but with type hints.
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::routine::tests::MockLine;
    use crate::sync::Source;
    use crate::nosync::Sink;
    use crate::{DefaultThread, Pushable};

    #[test]
    fn line_builder_reading_root() {
        let mut root = Root::<Message<usize, usize>, DefaultThread>::new();
        let mut source = Source::of(&root).unwrap();

        source.push(Message::Data(5)).unwrap();
        source.push(Message::Data(6)).unwrap();

        assert_eq!(root.pull().unwrap(), Message::Data(5));
        assert_eq!(root.pull().unwrap(), Message::Data(6));
    }

    #[test]
    fn line_builder_make_pull() {
        let root = Root::new();
        let mut source = Source::new(&root).unwrap();

        let line = make_pull(root, MockLine::new()).unwrap();
        let mut sink = Sink::new(line);

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.pull().unwrap(), Message::Data(2));
        assert_eq!(sink.pull().unwrap(), Message::Data(6));
    }
}
