use crate::error::Error;
use crate::signal::Visitors;
use crate::{marker::Connection, LineRoutine, Message, Origin};
use crate::{Pullable, SyncQueue, ThreadId};
use std::collections::VecDeque;
use std::sync::Arc;

pub struct Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, Message = Message<In, SignalType>>,
{
    pub visitors: Visitors,
    pub worker: LineRoutineType,

    pub pullable: Option<PullableType>,
    pub buffer: VecDeque<Message<Out, SignalType>>,

    pub input: Arc<SyncQueue<Message<In, SignalType>>>,
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType> Connection
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, Message = Message<In, SignalType>>,
{
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType> Pullable
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    ThreadIdType: ThreadId,
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, Message = Message<In, SignalType>>,
{
    type ThreadId = ThreadIdType;
    type Message = Message<Out, SignalType>;

    fn pull(&mut self) -> Result<Self::Message, Error> {
        // Push anything from our outstanding buffer.
        match self.next_output()? {
            Some(message) => {
                // Push to any potential connected pushable
                return Ok(message);
            }
            None => (),
        }

        // TODO implement resume, we will need this for generative models.
        // to allow a routine to emit partial results and then resume computation
        // on the same input.
        let _resume = 1;

        loop {
            let input_is_empty = self.input.is_empty()?;
            if !input_is_empty || self.pullable.is_some() {
                let input_object;
                match &mut self.pullable {
                    // We prioritize taking data from `Pullable` if it exists.
                    Some(pullable) => input_object = pullable.pull()?,
                    None => input_object = self.input.read_front()?,
                }
                // If we have some input we work on it.

                // Do work on our input or forward signals from input to output.
                match input_object {
                    Message::Data(data) => {
                        self.worker.work(data.clone())?;

                        match self.next_output()? {
                            Some(message) => {
                                // Push to any potential connected pushable
                                return Ok(message);
                            }
                            None => (),
                        }
                    }
                    Message::Flush(origin) => {
                        self.worker.flush()?;

                        match self.next_output()? {
                            Some(message) => {
                                // On next call we forward the flush.
                                self.buffer.push_back(Message::Flush(origin));

                                return Ok(message);
                            }
                            None => return Ok(Message::Flush(origin)),
                        }
                    }
                    Message::Marker(origin) => {
                        return Ok(Message::Marker(origin));
                    }
                }
                continue;
            }

            // If we did not have a `Pullable` input and there was no other input ready
            // we wait until there is some input ready.
            self.input.wait_front()?;
        }
    }
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
    Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    ThreadIdType: ThreadId,
    In: Clone + Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, Message = Message<In, SignalType>>,
{
    pub fn root(worker: LineRoutineType) -> Self {
        Line {
            visitors: Visitors::new(),
            worker,
            pullable: None,
            buffer: VecDeque::new(),
            input: Arc::new(SyncQueue::new()),
        }
    }
    pub fn new(worker: LineRoutineType, pullable: Option<PullableType>) -> Self {
        Line {
            visitors: Visitors::new(),
            worker,
            pullable,
            buffer: VecDeque::new(),
            input: Arc::new(SyncQueue::new()),
        }
    }

    fn next_output(&mut self) -> Result<Option<Message<Out, SignalType>>, Error> {
        // If we have output in our worker queue just immediately return it.
        match self.worker.output().pop_front() {
            Some(next_data) => {
                self.visitors.clear();
                return Ok(Some(Message::Data(next_data)));
            }
            None => (),
        }

        // If we have messages left in the node buffer, return it.
        match self.buffer.pop_front() {
            Some(next_msg) => {
                self.visitors.clear();
                return Ok(Some(next_msg));
            }
            None => (),
        }

        return Ok(None);
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::nosync::builder::{make_pull, root};
    use crate::node::line::routine::tests::MockLine;
    use crate::nosync::Sink;
    use crate::sync::Source;
    use crate::Pushable;
    use std::time::Instant;

    #[test]
    fn line_nosync_basic_run() {
        let line = root(MockLine::new()).unwrap();
        let mut source = Source::new(&line.input.clone()).unwrap();
        let mut sink = Sink::new(line);

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.pull().unwrap(), Message::Data(2));
        assert_eq!(sink.pull().unwrap(), Message::Data(6));

        // Reset processing
        source.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(sink.pull().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        assert_eq!(sink.pull().unwrap(), Message::Data(4));
    }

    #[test]
    fn line_nosync_basic_stacked() {
        let root = root(MockLine::new()).unwrap();

        let input = root.input.clone();

        let line1 = make_pull(root, MockLine::new()).unwrap();
        let line2 = make_pull(line1, MockLine::new()).unwrap();
        let mut source = Source::new(&input).unwrap();
        let mut sink = Sink::new(line2);

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.pull().unwrap(), Message::Data(8));
        assert_eq!(sink.pull().unwrap(), Message::Data(40));

        // Reset processing
        source.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(sink.pull().unwrap(), Message::Flush("hi".into()));

        source.push(Message::Data(2)).unwrap();
        assert_eq!(sink.pull().unwrap(), Message::Data(16));
    }

    #[ignore]
    #[test]
    fn line_nosync_basic_many_stack_benchmark() {
        let root = root(MockLine::new()).unwrap();

        let input = root.input.clone();

        let p1 = make_pull(root, MockLine::new()).unwrap();
        let p2 = make_pull(p1, MockLine::new()).unwrap();
        let p3 = make_pull(p2, MockLine::new()).unwrap();
        let p4 = make_pull(p3, MockLine::new()).unwrap();
        let p5 = make_pull(p4, MockLine::new()).unwrap();
        let p6 = make_pull(p5, MockLine::new()).unwrap();
        let p7 = make_pull(p6, MockLine::new()).unwrap();
        let p8 = make_pull(p7, MockLine::new()).unwrap();
        let p9 = make_pull(p8, MockLine::new()).unwrap();
        let p10 = make_pull(p9, MockLine::new()).unwrap();

        let mut source = Source::new(&input).unwrap();
        let mut sink = Sink::new(p10);

        let before = Instant::now();
        for _ in 0..10000 {
            source.push(Message::Data(1)).unwrap();
            source.push(Message::Flush("hi".into())).unwrap();

            assert_eq!(sink.pull().unwrap(), Message::Data(2048));
            assert_eq!(sink.pull().unwrap(), Message::Flush("hi".into()));
        }
        println!("Elapsed time: {:.2?}", before.elapsed());
    }

    #[test]
    fn line_nosync_respects_signal_ordering() {
        let root = root(MockLine::new()).unwrap();

        let input = root.input.clone();

        let line1 = make_pull(root, MockLine::new()).unwrap();
        let line2 = make_pull(line1, MockLine::new()).unwrap();
        let mut source = Source::of(&input).unwrap();
        let mut sink = Sink::new(line2);

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Flush(1)).unwrap();
        source.push(Message::Data(2)).unwrap();
        source.push(Message::Flush(2)).unwrap();
        source.push(Message::Data(3)).unwrap();
        source.push(Message::Marker(3)).unwrap();
        source.push(Message::Data(4)).unwrap();
        source.push(Message::Flush(4)).unwrap();

        assert_eq!(sink.pull().unwrap(), Message::Data(8));
        assert_eq!(sink.pull().unwrap(), Message::Flush(1));
        assert_eq!(sink.pull().unwrap(), Message::Data(16));
        assert_eq!(sink.pull().unwrap(), Message::Flush(2));
        assert_eq!(sink.pull().unwrap(), Message::Data(24));
        assert_eq!(sink.pull().unwrap(), Message::Marker(3));
        assert_eq!(sink.pull().unwrap(), Message::Data(104));
        assert_eq!(sink.pull().unwrap(), Message::Flush(4));
    }
}
