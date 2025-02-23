//! [LineTrait] and default implementation for running [LineRoutine].
use crate::error::Error;
use crate::node::line::LineRoutine;
use crate::signal::Visitors;
use crate::{
    graph::{Add, Get},
    marker::Connection,
    DefaultThread, SyncQueue, ThreadId,
};
use crate::{Message, Origin};
use crate::{Pushable, Workable};
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a line.
/// [`LineTrait`] describes the contract of this Node.
///
/// - The node is [Workable]
/// - We can [Add] a [Workable] to it.
/// - We can [Add] [Pushable] to it, outbound connections to send data.
/// - We can [Get] a [Pushable] from it. Inbound connection to recieve data.
///
/// It forms the basis if a node that recieves one input stream of data and
/// produces one output stream of data using a [LineRoutine].
///
/// Areamy provides a default implementation [Line], but users are
/// encouraged to implement [LineTrait] whenever [Line] does not
/// suit their needs.

pub trait LineTrait:
    // We can work on the line to produce output.
    Workable
    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId>>
    // We can add edges it should push into.
    + Add<dyn Pushable<Message = Message<Self::Out, Self::Signal>>>
    // We can retrieve its edge for others to push into.
    + Get<dyn Pushable<Message =  Message<Self::In, Self::Signal>>>
{
    /// The input data going into the line.
    type In: Clone + Send + Sync + 'static;
    /// The output data leaving it.
    type Out: Clone + Send + Sync;
    /// The signal type used in the graph.
    type Signal: Origin + Clone + 'static;
    /// The coroutine associated with this node.
    type LineRoutine: LineRoutine<Self::In, Self::Out>;

}

///  A Send+Sync+Clone variant of our [LineTrait] for types that implement it.
impl<LineType: LineTrait> LineTrait for Arc<Mutex<LineType>> {
    type In = LineType::In;
    type Out = LineType::Out;
    type Signal = LineType::Signal;
    type LineRoutine = LineType::LineRoutine;
}

/// [`Line`] is a default implementation of [LineTrait]. See [Line::work]
/// for implementation details on how it works.
///
/// If the implementation is not suitable for a usecase then implementing
/// your own [LineTrait] will let it be used as a drop-in replacement
/// for [Line].
pub struct Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
{
    /// State to keep track of signal visitors. It is used
    /// to support signal passing in e.g. cyclic graphs.
    pub visitors: Visitors,
    /// Worker or `Coroutine` associated with the current node.
    pub worker: LineRoutineType,

    /// Parent nodes that we can schedule to work.
    pub workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>,

    /// Edges that we can push data into.
    pub pushes: Vec<Box<dyn Pushable<Message = Message<Out, SignalType>>>>,

    /// Input to our current node that parents will push into.
    pub input: Arc<SyncQueue<Message<In, SignalType>>>,
}

/// Mark our Line as a possible connection in a graph. It's a connection
/// because it is `Workable`.
impl<In, Out, SignalType, ThreadIdType, LineRoutineType> Connection
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
{
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType> Workable
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    ThreadIdType: ThreadId + Clone,
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    LineRoutineType: LineRoutine<In, Out>,
{
    /// [Line::work] will produce output into all its [Line::pushes] when
    /// [Line::work] is invoked.
    ///
    /// It will prioritize returning [Line::worker] output from the last time
    /// it was scheduled. If there's no buffered output it will create new
    /// by working more on the [Line::worker] or by scheduling [Line::workers]
    /// to recieve additional input.
    fn work(&mut self) -> Result<(), Error> {
        // First we try to push any available output from our workers buffer.
        match self.next_output()? {
            Some(message) => {
                return self.push(message);
            }
            None => (),
        }

        let mut push_ok = false;

        // Work until we have some output to push into children.
        while !push_ok {
            // Poll for input.
            match self.input.poll()? {
                // If there's input forward to coroutine and output any new things.
                Some(input) => match input {
                    Message::Data(data) => {
                        self.worker.send(data)?;

                        // Try to push after performing the work, to see if we got something.
                        push_ok = match self.next_output()? {
                            Some(message) => {
                                self.push(message)?;
                                true
                            }
                            None => false,
                        }
                    }
                    Message::Flush(origin) => {
                        self.worker.flush()?;

                        // Try to push after flush to see if we got something
                        while let Some(message) = self.next_output()? {
                            self.push(message)?;
                        }

                        // Maybe forward the flush
                        push_ok = self.maybe_flush(&origin)?;
                    }
                    Message::Marker(origin) => {
                        push_ok = self.maybe_mark(&origin)?;
                    }
                },

                // If no input make parents work in hopes of producing input.
                None => {
                    // If there's no parents we just wait forever hoping someone gives us smt.
                    if self.workers.is_empty() {
                        self.input.wait_front()?;
                    }

                    // Then we work input from each source once.
                    for workable in self.workers.iter_mut() {
                        workable.work()?;
                    }
                }
            }
        }

        Ok(())
    }

    type ThreadId = ThreadIdType;
}

impl<In, Out, SignalType, LineRoutineType> Line<In, Out, SignalType, DefaultThread, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    LineRoutineType: LineRoutine<In, Out>,
{
    /// Create a [Line] owned by the [DefaultThread] with routine [LineRoutine].
    ///
    /// * `worker` - A [LineRoutine] that will transform data in this node.
    pub fn new(worker: LineRoutineType) -> Self {
        Line {
            visitors: Visitors::new(),
            worker,
            workers: Vec::new(),
            pushes: Vec::new(),
            input: Arc::new(SyncQueue::new()),
        }
    }
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType>
    Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
{
    /// Create a [Line] with routine [LineRoutine].
    ///
    /// * `worker` - A [LineRoutine] that will transform data in this node.
    pub fn of(worker: LineRoutineType) -> Self {
        Line {
            visitors: Visitors::new(),
            worker,
            workers: Vec::new(),
            pushes: Vec::new(),
            input: Arc::new(SyncQueue::new()),
        }
    }

    /// Maybe forward a flush signal. If the signal has travelled in a loop
    /// we ignore it and stop its graph traversal.
    fn maybe_flush(&mut self, flush: &SignalType) -> Result<bool, Error> {
        // If we already visited. We do not propagate.
        if self.visitors.contains(flush) {
            return Ok(false);
        }

        self.visitors.insert(flush);
        self.push(Message::Flush(flush.clone()))?;

        Ok(true)
    }

    /// Maybe forward a mark signal. If the signal has travelled in a loop
    /// we ignore it and stop its graph traversal.
    fn maybe_mark(&mut self, mark: &SignalType) -> Result<bool, Error> {
        // If we already visited. We do not propagate.
        if self.visitors.contains(mark) {
            return Ok(false);
        }

        self.visitors.insert(mark);
        self.push(Message::Marker(mark.clone()))?;

        Ok(true)
    }

    /// Get buffered output and reset signal traversal buffers.
    fn next_output(&mut self) -> Result<Option<Message<Out, SignalType>>, Error> {
        // If we have output in our worker queue just immediately return it.
        match self.worker.next()? {
            Some(next_message) => {
                self.visitors.clear();

                Ok(Some(Message::Data(next_message)))
            }
            None => Ok(None),
        }
    }

    /// push output into all [Pushable::push] edges.
    fn push(&mut self, obj: Message<Out, SignalType>) -> Result<(), Error> {
        for pushable in self.pushes.iter_mut() {
            pushable.push(obj.clone())?;
        }

        Ok(())
    }
}

/// Implement [LineTrait] for [Line].
/// It's mostly a type mapping after we've implemented
/// all the supertraits.
impl<In, Out, SignalType, ThreadIdType, LineRoutineType> LineTrait
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + Clone,
    LineRoutineType: LineRoutine<In, Out>,
{
    type In = In;
    type Out = Out;
    type Signal = SignalType;
    type LineRoutine = LineRoutineType;
}

/// Implement the [Get] constructor for inbound [Pushable] edge.
/// This allows users to get the input connection of this node.
impl<In, Out, SignalType, ThreadIdType, LineRoutineType>
    Get<dyn Pushable<Message = Message<In, SignalType>>>
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
{
    fn get(&self) -> Result<Box<dyn Pushable<Message = Message<In, SignalType>>>, Error> {
        Get::get(&self.input)
    }
}

/// Implement the [Add] constructor for inbound [Workable] edge.
/// This allows users to add [Workable] edges to this node such that it can schedule them.
impl<In, Out, SignalType, ThreadIdType, LineRoutineType> Add<dyn Workable<ThreadId = ThreadIdType>>
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + Clone,
    LineRoutineType: LineRoutine<In, Out>,
{
    fn add(&mut self, workable: Box<dyn Workable<ThreadId = ThreadIdType>>) -> Result<(), Error> {
        Ok(self.workers.push(workable))
    }
}

/// Implement the [Add] constructor for output [Pushable] edge.
/// This allows users to add [Pushable] edges to this node such that it can [Pushable::push] data
/// into them when scheduled.
impl<In, Out, SignalType, ThreadIdType, LineRoutineType>
    Add<dyn Pushable<Message = Message<Out, SignalType>>>
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + Clone,
    LineRoutineType: LineRoutine<In, Out>,
{
    fn add(
        &mut self,
        pushable: Box<dyn Pushable<Message = Message<Out, SignalType>>>,
    ) -> Result<(), Error> {
        Ok(self.pushes.push(pushable))
    }
}

/// [make_line] creates a [LineTrait] implementation from a [LineRoutine] this
/// can then be connected to other graph nodes using the functions such as
/// - [crate::make_bidi]
/// - [crate::make_push]
/// - [crate::make_work]
pub fn make_line<In, Out, SignalType, ThreadIdType, RoutineType>(
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::routine::tests::{AccMockLine, MockLine, MockWaitLine};
    use crate::{make_bidi, work::make_line};
    use crate::{sink::work::tee, work::Connect, work::Sink, work::Source};
    use std::time::Instant;

    #[test]
    fn line_accumulating_node_works() {
        let line = make_line(AccMockLine::new()).unwrap();
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(vec![1, 2]));
    }

    #[test]
    fn line_wait_node_mark_waits() {
        let line = make_line(MockWaitLine::new(4)).unwrap();
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();
        source.push(Message::Data(3)).unwrap();

        // Send a marker
        source.push(Message::Marker("no_output".into())).unwrap();
        // We get marker because no output is ready.
        assert_eq!(sink.read().unwrap(), Message::Marker("no_output".into()));

        // Send data so that node releases all data.
        source.push(Message::Data(4)).unwrap();

        // Send marker.
        source.push(Message::Marker("output!".into())).unwrap();

        // Now we get all data in order and marker last.
        assert_eq!(sink.read().unwrap(), Message::Data(1));
        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Data(3));
        assert_eq!(sink.read().unwrap(), Message::Data(4));
        // The marker will arrive last!
        assert_eq!(sink.read().unwrap(), Message::Marker("output!".into()));
    }

    #[test]
    fn line_wait_node_flush_waits() {
        let line = make_line(MockWaitLine::new(4)).unwrap();
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();
        source.push(Message::Data(3)).unwrap();
        source.push(Message::Flush("force_output".into())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(1));
        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Data(3));
        // The flush will arrive last!
        assert_eq!(sink.read().unwrap(), Message::Flush("force_output".into()));
    }

    #[test]
    fn line_basic_run() {
        let line = make_line(MockLine::new()).unwrap();
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Data(6));

        // Reset processing
        source.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn line_can_mark() {
        let line = make_line(MockLine::new()).unwrap();
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        // One data
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Marker("hi".into())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Marker("hi".into()));
    }

    #[test]
    fn line_can_flush() {
        let line = make_line(MockLine::new()).unwrap();
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        // One data
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Flush("hi".into())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));
    }

    #[test]
    fn line_can_be_stacked() {
        let line_1 = make_line(MockLine::new()).unwrap();
        let mut line_2 = make_line(MockLine::new()).unwrap();

        let mut source = Source::new(&line_1).unwrap();
        make_bidi(line_1, &mut line_2).unwrap();

        let mut sink = Sink::new(line_2).unwrap();

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(4));
        assert_eq!(sink.read().unwrap(), Message::Data(16));

        // Reset processing
        source.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(8));
    }

    #[test]
    fn line_can_be_stacked_with_type_hints() {
        let line_1 = make_line(MockLine::new()).unwrap();
        let mut line_2 = make_line(MockLine::new()).unwrap();

        let mut source = Source::new(&line_1).unwrap();

        // This typehint is not needed as exemplified by other tests
        // but it helps readability to be explicit when building
        // the graph.
        Connect::<usize>::bidi(line_1, &mut line_2).unwrap();

        let mut sink = Sink::new(line_2).unwrap();

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(4));
        assert_eq!(sink.read().unwrap(), Message::Data(16));

        // Reset processing
        source.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(8));
    }

    #[test]
    fn line_can_tee() {
        // TODO make a tee sink where the sink only gets pushable references
        // but does not own the workable.
        let mut line = make_line(MockLine::new()).unwrap();

        let mut source = Source::new(&line).unwrap();
        let mut sink_1 = tee::Sink::new(line.as_mut()).unwrap();
        let mut sink_2 = tee::Sink::new(line.as_mut()).unwrap();

        let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = line;

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        workable.work().unwrap();
        workable.work().unwrap();

        assert_eq!(sink_1.read().unwrap(), Message::Data(2));
        assert_eq!(sink_1.read().unwrap(), Message::Data(6));

        assert_eq!(sink_2.read().unwrap(), Message::Data(2));
        assert_eq!(sink_2.read().unwrap(), Message::Data(6));

        // Reset processing
        source.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        workable.work().unwrap();
        assert_eq!(sink_1.read().unwrap(), Message::Flush("hi".into()));
        assert_eq!(sink_2.read().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        workable.work().unwrap();
        assert_eq!(sink_1.read().unwrap(), Message::Data(4));
        assert_eq!(sink_2.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn line_can_merge() {
        let line = make_line(MockLine::new()).unwrap();

        let mut source_1 = Source::new(&line).unwrap();
        let mut source_2 = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        // Add one flush
        source_1.push(Message::Data(1)).unwrap();
        source_2.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Data(6));

        // Note for merging the markers have to have different
        // IDs otherwise they'll be treated as duplicates
        // and stopped.
        source_2.push(Message::Marker("source_2".into())).unwrap();
        source_1.push(Message::Flush("source_1".into())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Marker("source_2".into()));
        assert_eq!(sink.read().unwrap(), Message::Flush("source_1".into()));

        source_1.push(Message::Data(2)).unwrap();
        source_2.push(Message::Data(1)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(4));
        assert_eq!(sink.read().unwrap(), Message::Data(6));
    }

    #[ignore]
    #[test]
    fn line_basic_many_stack_benchmark() {
        let line_0 = make_line(MockLine::new()).unwrap();

        let mut source = Source::new(&line_0).unwrap();

        let mut line_1 = make_line(MockLine::new()).unwrap();
        let mut line_2 = make_line(MockLine::new()).unwrap();
        let mut line_3 = make_line(MockLine::new()).unwrap();
        let mut line_4 = make_line(MockLine::new()).unwrap();
        let mut line_5 = make_line(MockLine::new()).unwrap();
        let mut line_6 = make_line(MockLine::new()).unwrap();
        let mut line_7 = make_line(MockLine::new()).unwrap();
        let mut line_8 = make_line(MockLine::new()).unwrap();
        let mut line_9 = make_line(MockLine::new()).unwrap();
        let mut line_10 = make_line(MockLine::new()).unwrap();

        make_bidi(line_0, &mut line_1).unwrap();
        make_bidi(line_1, &mut line_2).unwrap();
        make_bidi(line_2, &mut line_3).unwrap();
        make_bidi(line_3, &mut line_4).unwrap();
        make_bidi(line_4, &mut line_5).unwrap();
        make_bidi(line_5, &mut line_6).unwrap();
        make_bidi(line_6, &mut line_7).unwrap();
        make_bidi(line_7, &mut line_8).unwrap();
        make_bidi(line_8, &mut line_9).unwrap();
        make_bidi(line_9, &mut line_10).unwrap();

        let mut sink = Sink::new(line_10).unwrap();

        let before = Instant::now();
        for _ in 0..10000 {
            source.push(Message::Data(1)).unwrap();
            source.push(Message::Flush("hi".into())).unwrap();

            assert_eq!(sink.read().unwrap(), Message::Data(2048));
            assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));
        }
        println!("Elapsed time: {:.2?}", before.elapsed());
    }
}
