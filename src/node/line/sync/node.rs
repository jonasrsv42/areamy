use crate::error::Error;
use crate::node::line::LineRoutine;
use crate::signal::Visitors;
use crate::{AddPushable, AddWorkable, DefaultThread, GetPushable, SyncQueue, ThreadId};
use crate::{Message, Origin};
use crate::{Pushable, Workable};
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a line.
pub trait LineTrait:
    // We can work on the line to produce output.
    Workable
    // We can add things for it to work on, parents nodes.
    + AddWorkable<ThreadId = <Self as Workable>::ThreadId>
    // We can add edges it should push into.
    + AddPushable<Message = Message<Self::Out, Self::Signal>>
    // We can retrieve its edge for others to push into.
    + GetPushable<Pushable = Arc<SyncQueue<Message<Self::In, Self::Signal>>>>
{
    // The input data going into the line.
    type In: Clone + Send + Sync + 'static;
    // The output data leaving it..
    type Out: Clone + Send + Sync;
    // The signal type used in the graph.
    type Signal: Origin + Clone + 'static;
    // The coroutine associated with this node.
    type LineRoutine: LineRoutine<Self::In, Self::Out>;

}

impl<LineType: LineTrait> LineTrait for Arc<Mutex<LineType>> {
    type In = LineType::In;
    type Out = LineType::Out;
    type Signal = LineType::Signal;
    type LineRoutine = LineType::LineRoutine;
}

pub struct Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
{
    // State to keep track of signal visitors. It is used
    // to support signal passing in e.g. cyclic graphs.
    pub visitors: Visitors,
    // Worker or `Coroutine` associated with the current node.
    pub worker: LineRoutineType,

    // Parent nodes that we can pull on.
    pub workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>,

    // Child inputs that we can push into.
    pub pushes: Vec<Box<dyn Pushable<Message = Message<Out, SignalType>>>>,

    // Input to our current node that parents will push into.
    pub input: Arc<SyncQueue<Message<In, SignalType>>>,
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
    fn work(&mut self) -> Result<(), Error> {
        // First we try to push any available output from our workers buffer.
        match self.next_output()? {
            Some(message) => {
                return self.push(message);
            }
            None => (),
        }

        // TODO implement reusme
        let resume = 1;

        let mut push_ok = false;
        // Otherwise we loop until we have some output.
        // To produce output we work on all the input
        // or request more input by working.
        while !push_ok {
            let input_is_empty = self.input.is_empty()?;

            if !input_is_empty {
                let input_object = self.input.read_front()?;
                // If we have some input we work on it.

                // Do work on our input or forward signals from input to output.
                match input_object {
                    Message::Data(data) => {
                        self.worker.work(data.clone())?;

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
                        match self.next_output()? {
                            Some(message) => {
                                self.push(message)?;
                            }
                            None => (),
                        };

                        // Maybe forward the flush
                        push_ok = self.maybe_flush(&origin)?;
                    }
                    Message::Marker(origin) => {
                        push_ok = self.maybe_mark(&origin)?;
                    }
                }

                // Continue to avoid unecessary work.
                continue;
            }

            // NOTE: this part is unreachable if our node has `Pullable` input.
            // as it will always pull it until it gets something.
            // This following part is for nodes without nosync `Pullable` input.
            //
            // If there were no available input we grab ownership of our
            // sources and work them.
            if self.workers.is_empty() {
                // Just wait until there is an element.
                self.input.wait_front()?;
            }

            // Then we work input from each source once.
            for workable in self.workers.iter_mut() {
                workable.work()?;
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
    pub fn of(worker: LineRoutineType) -> Self {
        Line {
            visitors: Visitors::new(),
            worker,
            workers: Vec::new(),
            pushes: Vec::new(),
            input: Arc::new(SyncQueue::new()),
        }
    }

    fn maybe_flush(&mut self, flush: &SignalType) -> Result<bool, Error> {
        // If we already visited. We do not propagate.
        if self.visitors.contains(flush) {
            return Ok(false);
        }

        self.visitors.insert(flush);
        self.push(Message::Flush(flush.clone()))?;

        Ok(true)
    }

    fn maybe_mark(&mut self, mark: &SignalType) -> Result<bool, Error> {
        // If we already visited. We do not propagate.
        if self.visitors.contains(mark) {
            return Ok(false);
        }

        self.visitors.insert(mark);
        self.push(Message::Marker(mark.clone()))?;

        Ok(true)
    }

    fn next_output(&mut self) -> Result<Option<Message<Out, SignalType>>, Error> {
        // If we have output in our worker queue just immediately return it.
        match self.worker.output().pop_front() {
            Some(next_message) => {
                self.visitors.clear();

                Ok(Some(Message::Data(next_message)))
            }
            None => Ok(None),
        }
    }

    fn push(&mut self, obj: Message<Out, SignalType>) -> Result<(), Error> {
        for pushable in self.pushes.iter_mut() {
            pushable.push(obj.clone())?;
        }

        Ok(())
    }
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType> GetPushable
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
{
    type Pushable = Arc<SyncQueue<Message<In, SignalType>>>;

    fn get(&self) -> Result<Self::Pushable, Error> {
        Ok(self.input.clone())
    }
}

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

impl<In, Out, SignalType, ThreadIdType, LineRoutineType> AddWorkable
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + Clone,
    LineRoutineType: LineRoutine<In, Out>,
{
    type ThreadId = ThreadIdType;
    fn add<WorkableType>(&mut self, workable: WorkableType) -> Result<(), Error>
    where
        WorkableType: Workable<ThreadId = ThreadIdType> + 'static,
    {
        Ok(self.workers.push(Box::new(workable)))
    }
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType> AddPushable
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + Clone,
    LineRoutineType: LineRoutine<In, Out>,
{
    type Message = Message<Out, SignalType>;
    fn add<PushableType>(&mut self, pushable: PushableType) -> Result<(), Error>
    where
        PushableType: Pushable<Message = Message<Out, SignalType>> + 'static,
    {
        Ok(self.pushes.push(Box::new(pushable)))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::routine::tests::MockLine;
    use crate::{make_bidi, sync::make_line};
    use crate::{sync::Connect, sync::Sink, sync::Source};
    use std::time::Instant;

    #[test]
    fn line_basic_run() {
        let mut line = make_line(MockLine::new()).unwrap();
        let mut source = Source::new(line.input()).unwrap();
        let mut sink = Sink::new(line.workable(), line.output()).unwrap();

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
        let mut line = make_line(MockLine::new()).unwrap();
        let mut source = Source::new(line.input()).unwrap();
        let mut sink = Sink::new(line.workable(), line.output()).unwrap();

        // One data
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Marker("hi".into())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Marker("hi".into()));
    }

    #[test]
    fn line_can_flush() {
        let mut line = make_line(MockLine::new()).unwrap();
        let mut source = Source::new(line.input()).unwrap();
        let mut sink = Sink::new(line.workable(), line.output()).unwrap();

        // One data
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Flush("hi".into())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));
    }

    #[test]
    fn line_can_be_stacked() {
        let mut line_1 = make_line(MockLine::new()).unwrap();
        let mut line_2 = make_line(MockLine::new()).unwrap();

        make_bidi(&mut line_1, &mut line_2).unwrap();

        let mut source = Source::new(line_1.input()).unwrap();
        let mut sink = Sink::new(line_2.workable(), line_2.output()).unwrap();

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
        let mut line_1 = make_line(MockLine::new()).unwrap();
        let mut line_2 = make_line(MockLine::new()).unwrap();

        // This typehint is not needed as exemplified by other tests
        // but it helps readability to be explicit when building
        // the graph.
        Connect::<usize>::bidi(&mut line_1, &mut line_2).unwrap();

        let mut source = Source::new(line_1.input()).unwrap();
        let mut sink = Sink::new(line_2.workable(), line_2.output()).unwrap();

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
        let mut line = make_line(MockLine::new()).unwrap();

        let mut source = Source::new(line.input()).unwrap();
        let mut sink_1 = Sink::new(line.workable(), line.output()).unwrap();
        let mut sink_2 = Sink::new(line.workable(), line.output()).unwrap();

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink_1.read().unwrap(), Message::Data(2));
        assert_eq!(sink_1.read().unwrap(), Message::Data(6));

        assert_eq!(sink_2.read().unwrap(), Message::Data(2));
        assert_eq!(sink_2.read().unwrap(), Message::Data(6));

        // Reset processing
        source.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(sink_1.read().unwrap(), Message::Flush("hi".into()));
        assert_eq!(sink_2.read().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        assert_eq!(sink_1.read().unwrap(), Message::Data(4));
        assert_eq!(sink_2.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn line_can_merge() {
        let mut line = make_line(MockLine::new()).unwrap();

        let mut source_1 = Source::new(line.input()).unwrap();
        let mut source_2 = Source::new(line.input()).unwrap();
        let mut sink = Sink::new(line.workable(), line.output()).unwrap();

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
        let mut line_0 = make_line(MockLine::new()).unwrap();
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

        make_bidi(&mut line_0, &mut line_1).unwrap();
        make_bidi(&mut line_1, &mut line_2).unwrap();
        make_bidi(&mut line_2, &mut line_3).unwrap();
        make_bidi(&mut line_3, &mut line_4).unwrap();
        make_bidi(&mut line_4, &mut line_5).unwrap();
        make_bidi(&mut line_5, &mut line_6).unwrap();
        make_bidi(&mut line_6, &mut line_7).unwrap();
        make_bidi(&mut line_7, &mut line_8).unwrap();
        make_bidi(&mut line_8, &mut line_9).unwrap();
        make_bidi(&mut line_9, &mut line_10).unwrap();

        let mut source = Source::new(line_0.input()).unwrap();
        let mut sink = Sink::new(line_10.workable(), line_10.output()).unwrap();

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
