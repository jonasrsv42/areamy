use crate::error::Error;
use crate::{LineRoutine, Message, Origin, marker::Connection};
use crate::{Pullable, ThreadId};
use std::collections::VecDeque;

/// [`Line`] with a [Pullable] connection that avoids dynamic dispatch.
/// The line node is well suited to be included in line segments of a graph
/// where synchroniation is not necessary. It will [Pullable::pull] its parent
/// and process its [LineRoutine] until it produces some output.
pub struct Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    In: Send + Sync,
    Out: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, DataType = In, SignalType = SignalType>,
{
    /// The [LineRoutine] worker in our node, responsible for transforming input when
    /// scheduled.
    pub worker: LineRoutineType,

    /// A parent [Pullable].
    pub pullable: PullableType,

    /// Buffer to maintain messages that will be forwarded to a child that pulls
    /// on the next pull.
    pub buffer: VecDeque<Message<Out, SignalType>>,
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType> Connection
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    In: Send + Sync,
    Out: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, DataType = In, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType> Pullable
    for Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    ThreadIdType: ThreadId,
    In: Send + Sync,
    Out: Send + Sync,
    SignalType: Origin + Send + Sync,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, DataType = In, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;
    type DataType = Out;
    type SignalType = SignalType;

    /// [Line::pull] yields the next output provided on [Pullable::pull]. It will
    /// prioritize to yield output from the [Line::worker] buffer or its own
    /// [Line::buffer]. If there's no data it'll try to create by processing
    /// [Line::pullable] input.
    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        // Push anything from our outstanding buffer.
        match self.next_output()? {
            Some(message) => {
                // Push to any potential connected pushable
                return Ok(message);
            }
            None => (),
        }

        loop {
            // Pull until we manage to produce an output
            match self.pullable.pull()? {
                Message::Data(data) => {
                    self.worker.send(data)?;

                    match self.next_output()? {
                        Some(message) => {
                            // Return to output connections.
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
        }
    }
}

impl<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
    Line<In, Out, SignalType, ThreadIdType, LineRoutineType, PullableType>
where
    ThreadIdType: ThreadId,
    In: Send + Sync,
    Out: Send + Sync,
    SignalType: Origin + Send + Sync,
    LineRoutineType: LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, DataType = In, SignalType = SignalType>,
{
    /// Create a new [Line] from a parent [Pullable] and a [LineRoutine] to operate
    /// in this node.
    pub fn new(worker: LineRoutineType, pullable: PullableType) -> Self {
        Line {
            worker,
            pullable,
            buffer: VecDeque::new(),
        }
    }

    /// [Line::next_output] yields the next buffered output provided on [Pullable::pull]
    /// if there's none we'll create new output by working on the [LineRoutine] or
    /// [Pullable::pull] parents.
    fn next_output(&mut self) -> Result<Option<Message<Out, SignalType>>, Error> {
        // If we have output in our worker queue just immediately return it.
        match self.worker.next()? {
            Some(next_data) => {
                return Ok(Some(Message::Data(next_data)));
            }
            None => (),
        }

        // If we have messages left in the node buffer, return it.
        match self.buffer.pop_front() {
            Some(next_msg) => {
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
    use crate::Pushable;
    use crate::node::line::pull::builder::make_pull;
    use crate::node::line::routine::tests::{AccMockLine, MockLine, MockWaitLine};
    use crate::pull::Reader;
    use crate::work::Writer;
    use crate::writer::pull::WriterBuffer;
    use std::time::Instant;

    #[test]
    fn line_accumulating_node_works() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();
        let line = make_pull(buffer, AccMockLine::new());
        let mut reader = Reader::new(line);

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.pull().unwrap(), Message::Data(vec![1, 2]));
    }

    #[test]
    fn line_nosync_basic_run() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();

        let line = make_pull(buffer, MockLine::new());
        let mut reader = Reader::new(line);

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.pull().unwrap(), Message::Data(2));
        assert_eq!(reader.pull().unwrap(), Message::Data(6));

        // Reset processing
        writer.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(reader.pull().unwrap(), Message::Flush("hi".into()));

        writer.push(Message::Data(2)).unwrap();
        assert_eq!(reader.pull().unwrap(), Message::Data(4));
    }

    #[test]
    fn line_wait_node_mark_waits() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();

        let line = make_pull(buffer, MockWaitLine::new(4));
        let mut reader = Reader::new(line);

        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Data(3)).unwrap();

        // Send a marker
        writer.push(Message::Marker("no_output".into())).unwrap();
        // We get marker because no output is ready.
        assert_eq!(reader.pull().unwrap(), Message::Marker("no_output".into()));

        // Send data so that node releases all data.
        writer.push(Message::Data(4)).unwrap();

        // Send marker.
        writer.push(Message::Marker("output!".into())).unwrap();

        // Now we get all data in order and marker last.
        assert_eq!(reader.pull().unwrap(), Message::Data(1));
        assert_eq!(reader.pull().unwrap(), Message::Data(2));
        assert_eq!(reader.pull().unwrap(), Message::Data(3));
        assert_eq!(reader.pull().unwrap(), Message::Data(4));
        // The marker will arrive last!
        assert_eq!(reader.pull().unwrap(), Message::Marker("output!".into()));
    }

    #[test]
    fn line_wait_node_flush_waits() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();

        let line = make_pull(buffer, MockWaitLine::new(4));
        let mut reader = Reader::new(line);

        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Data(3)).unwrap();
        writer.push(Message::Flush("force_output".into())).unwrap();

        assert_eq!(reader.pull().unwrap(), Message::Data(1));
        assert_eq!(reader.pull().unwrap(), Message::Data(2));
        assert_eq!(reader.pull().unwrap(), Message::Data(3));
        // The flush will arrive last!
        assert_eq!(
            reader.pull().unwrap(),
            Message::Flush("force_output".into())
        );
    }

    #[test]
    fn line_nosync_basic_stacked() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();

        let line1 = make_pull(buffer, MockLine::new());
        let line2 = make_pull(line1, MockLine::new());
        let mut reader = Reader::new(line2);

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        // 4  =
        // 1 * 2 = 2
        // 2 * 4 = 4
        assert_eq!(reader.pull().unwrap(), Message::Data(4));
        // 16  =
        // (2 + 1) * 2 = 6
        // (6 + 2) * 2 = 16
        assert_eq!(reader.pull().unwrap(), Message::Data(16));

        // Reset processing
        writer.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(reader.pull().unwrap(), Message::Flush("hi".into()));

        writer.push(Message::Data(2)).unwrap();

        // 8 =
        // 2 * 2 = 4
        // 4 * 2 = 8
        assert_eq!(reader.pull().unwrap(), Message::Data(8));
    }

    #[ignore]
    #[test]
    fn line_nosync_basic_many_stack_benchmark() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();

        let p1 = make_pull(buffer, MockLine::new());
        let p2 = make_pull(p1, MockLine::new());
        let p3 = make_pull(p2, MockLine::new());
        let p4 = make_pull(p3, MockLine::new());
        let p5 = make_pull(p4, MockLine::new());
        let p6 = make_pull(p5, MockLine::new());
        let p7 = make_pull(p6, MockLine::new());
        let p8 = make_pull(p7, MockLine::new());
        let p9 = make_pull(p8, MockLine::new());
        let p10 = make_pull(p9, MockLine::new());

        let mut reader = Reader::new(p10);

        let before = Instant::now();
        for _ in 0..10000 {
            writer.push(Message::Data(1)).unwrap();
            writer.push(Message::Flush("hi".into())).unwrap();

            assert_eq!(reader.pull().unwrap(), Message::Data(2048));
            assert_eq!(reader.pull().unwrap(), Message::Flush("hi".into()));
        }
        println!("Elapsed time: {:.2?}", before.elapsed());
    }

    #[test]
    fn line_nosync_respects_signal_ordering() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::of(&buffer).unwrap();

        let line1 = make_pull(buffer, MockLine::new());
        let line2 = make_pull(line1, MockLine::new());
        let mut reader = Reader::new(line2);

        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Flush(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Flush(2)).unwrap();
        writer.push(Message::Data(3)).unwrap();
        writer.push(Message::Marker(3)).unwrap();
        writer.push(Message::Data(4)).unwrap();
        writer.push(Message::Flush(4)).unwrap();

        assert_eq!(reader.pull().unwrap(), Message::Data(4));
        assert_eq!(reader.pull().unwrap(), Message::Flush(1));
        assert_eq!(reader.pull().unwrap(), Message::Data(8));
        assert_eq!(reader.pull().unwrap(), Message::Flush(2));
        assert_eq!(reader.pull().unwrap(), Message::Data(12));
        assert_eq!(reader.pull().unwrap(), Message::Marker(3));
        assert_eq!(reader.pull().unwrap(), Message::Data(40));
        assert_eq!(reader.pull().unwrap(), Message::Flush(4));
    }
}
