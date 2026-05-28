//! [LineTrait] and default implementation for running [LineRoutine].
use crate::connect::sync::Receiver;
use crate::error::Error;
use crate::node::line::LineRoutine;
use crate::{Closeable, Pushable, Workable};
use crate::{
    DefaultThread, ThreadId,
    graph::{Add, Get},
    marker::Connection,
};
use crate::{Message, Origin};
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a line.
/// [`LineTrait`] describes the contract of this Node.
///
/// - The node is [Workable]
/// - We can [Add] a [Workable] to it.
/// - We can [Add] [Closeable] to it, outbound connections to send data.
/// - We can [Get] a [Closeable] from it. Inbound connection to recieve data.
///
/// It forms the basis if a node that recieves one input stream of data and
/// produces one output stream of data using a [LineRoutine].
///
/// Areamy provides a default implementation [Line], but users are
/// encouraged to implement [LineTrait] whenever [Line] does not
/// suit their needs.

pub trait LineTrait<'params>:
    // We can work on the line to produce output.
    Workable
    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params>
    // We can add edges it should push into.
    + Add<dyn Closeable<DataType = Self::Out, SignalType = Self::Signal> + Send + Sync + 'params>
    // We can retrieve its edge for others to push into.
    + Get<dyn Closeable<DataType = Self::In, SignalType = Self::Signal> + Send + Sync + 'params>
{
    /// The input data going into the line.
    type In: Send + Sync + 'static;
    /// The output data leaving it.
    type Out: Clone + Send + Sync;
    /// The signal type used in the graph.
    type Signal: Origin + Clone + 'static;
    /// The coroutine associated with this node.
    type LineRoutine: LineRoutine<Self::In, Self::Out>;

}

///  A Send+Sync+Clone variant of our [LineTrait] for types that implement it.
impl<'params, LineType: LineTrait<'params>> LineTrait<'params> for Arc<Mutex<LineType>> {
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
pub struct Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + 'static,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    /// Worker or `Coroutine` associated with the current node.
    pub worker: LineRoutineType,

    /// Parent nodes that we can schedule to work.
    pub workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,

    /// Edges that we can push data into. Uses [Closeable] to support shutdown propagation.
    pub pushes:
        Vec<Box<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync + 'params>>,

    /// Input to our current node that parents will push into.
    pub input: Receiver<In, SignalType>,
}

/// Mark our Line as a possible connection in a graph. It's a connection
/// because it is `Workable`.
impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType> Connection
    for Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + 'static,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
}

impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType> Workable
    for Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    ThreadIdType: ThreadId,
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    LineRoutineType: LineRoutine<In, Out> + 'params,
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
            // Poll for input. Propagate close to push outputs if input is closed.
            match self.propagate_if_closed(self.input.poll())? {
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
                        self.push(Message::Flush(origin.clone()))?;
                        push_ok = true;
                    }
                    Message::Marker(origin) => {
                        self.push(Message::Marker(origin.clone()))?;
                        push_ok = true;
                    }
                },

                // If no input make parents work in hopes of producing input.
                None => {
                    // If there's no parents we just wait forever hoping someone gives us smt.
                    if self.workers.is_empty() {
                        self.propagate_if_closed(self.input.wait_front())?;
                    }

                    // Then we work input from each source once.
                    // Propagate close to push outputs if parent work returns closed.
                    // Use index-based loop to avoid borrow conflict with propagate_if_closed.
                    for i in 0..self.workers.len() {
                        let result = self.workers[i].work();
                        self.propagate_if_closed(result)?;
                    }
                }
            }
        }

        Ok(())
    }

    type ThreadId = ThreadIdType;
}

impl<'params, In, Out, SignalType, LineRoutineType>
    Line<'params, In, Out, SignalType, DefaultThread, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    /// Create a [Line] owned by the [DefaultThread] with routine [LineRoutine].
    ///
    /// * `worker` - A [LineRoutine] that will transform data in this node.
    pub fn new(worker: LineRoutineType) -> Self {
        Line {
            worker,
            workers: Vec::new(),
            pushes: Vec::new(),
            input: Receiver::new(),
        }
    }
}

impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
    Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + 'static,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    /// Create a [Line] with routine [LineRoutine].
    ///
    /// * `worker` - A [LineRoutine] that will transform data in this node.
    pub fn of(worker: LineRoutineType) -> Self {
        Line {
            worker,
            workers: Vec::new(),
            pushes: Vec::new(),
            input: Receiver::new(),
        }
    }

    /// Get buffered output and reset signal traversal buffers.
    fn next_output(&mut self) -> Result<Option<Message<Out, SignalType>>, Error> {
        // If we have output in our worker queue just immediately return it.
        match self.worker.next()? {
            Some(next_message) => Ok(Some(Message::Data(next_message))),
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

    /// Close all push outputs. Called on shutdown to propagate close through push connections.
    fn close_pushes(&mut self) {
        for pushable in self.pushes.iter_mut() {
            let _ = pushable.close();
        }
    }

    /// If result is a Closed error, close all pushes to propagate shutdown.
    /// Returns the original result unchanged.
    fn propagate_if_closed<T>(&mut self, result: Result<T, Error>) -> Result<T, Error> {
        result.map_err(|e| {
            if matches!(e.kind, crate::error::ErrorKind::Closed) {
                self.close_pushes();
            }
            e
        })
    }
}

/// Implement [LineTrait] for [Line].
/// It's mostly a type mapping after we've implemented
/// all the supertraits.
impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType> LineTrait<'params>
    for Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    type In = In;
    type Out = Out;
    type Signal = SignalType;
    type LineRoutine = LineRoutineType;
}

/// Get a [Closeable] for this node's input edge.
impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
    Get<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync + 'params>
    for Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync + 'params>,
        Error,
    > {
        Get::get(&self.input)
    }
}

/// Implement the [Add] constructor for inbound [Workable] edge.
/// This allows users to add [Workable] edges to this node such that it can schedule them.
impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType> + 'params>
    for Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + 'static,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    ) -> Result<(), Error> {
        Ok(self.workers.push(workable))
    }
}

/// Implement the [Add] constructor for output [Closeable] edge.
/// This allows users to add [Closeable] edges to this node such that it can push data
/// into them when scheduled, and close them on shutdown.
impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
    Add<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync + 'params>
    for Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId + 'static,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    fn add(
        &mut self,
        closeable: Box<
            dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync + 'params,
        >,
    ) -> Result<(), Error> {
        Ok(self.pushes.push(closeable))
    }
}

/// [make_line] creates a [LineTrait] implementation from a [LineRoutine] this
/// can then be connected to other graph nodes using the functions such as
/// - [crate::make_bidi]
/// - [crate::make_push]
/// - [crate::make_work]
///
/// Returns a concrete `Box<Line<...>>` rather than `Box<impl LineTrait + ...>`.
/// This is intentional — `impl Trait` return types prevent the compiler from
/// resolving supertrait bounds involving `dyn Trait + Send + Sync`, which
/// breaks type inference for `make_bidi`/`make_push` in downstream code.
pub fn make_line<'params, In, Out, SignalType, ThreadIdType, RoutineType>(
    worker: RoutineType,
) -> Box<Line<'params, In, Out, SignalType, ThreadIdType, RoutineType>>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: LineRoutine<In, Out> + 'params,
{
    Box::new(Line::<
        'params,
        In,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
    >::of(worker))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::node::line::routine::tests::{AccMockLine, MockLine, MockWaitLine};
    use crate::{make_bidi, work::make_line};
    use crate::{sink::work::tee, work::Sink, work::Source};
    use std::time::Instant;

    #[test]
    fn line_accumulating_node_works() {
        let line = make_line(AccMockLine::new());
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(vec![1, 2]));
    }

    #[test]
    fn line_wait_node_mark_waits() {
        let line = make_line(MockWaitLine::new(4));
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
        let line = make_line(MockWaitLine::new(4));
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
        let line = make_line(MockLine::new());
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
        let line = make_line(MockLine::new());
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
        let line = make_line(MockLine::new());
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
        let line_1 = make_line(MockLine::new());
        let mut line_2 = make_line(MockLine::new());

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
        let line_1 = make_line(MockLine::new());
        let mut line_2 = make_line(MockLine::new());

        let mut source = Source::new(&line_1).unwrap();

        // This typehint is not needed as exemplified by other tests
        // but it helps readability to be explicit when building
        // the graph.
        make_bidi(line_1, &mut line_2).unwrap();

        let mut sink = Sink::<usize>::new(line_2).unwrap();

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
        let mut line = make_line(MockLine::new());

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
        let line = make_line(MockLine::new());

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
        let line_0 = make_line(MockLine::new());

        let mut source = Source::new(&line_0).unwrap();

        let mut line_1 = make_line(MockLine::new());
        let mut line_2 = make_line(MockLine::new());
        let mut line_3 = make_line(MockLine::new());
        let mut line_4 = make_line(MockLine::new());
        let mut line_5 = make_line(MockLine::new());
        let mut line_6 = make_line(MockLine::new());
        let mut line_7 = make_line(MockLine::new());
        let mut line_8 = make_line(MockLine::new());
        let mut line_9 = make_line(MockLine::new());
        let mut line_10 = make_line(MockLine::new());

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

        let mut sink = Sink::<usize>::new(line_10).unwrap();

        let before = Instant::now();
        for _ in 0..10000 {
            source.push(Message::Data(1)).unwrap();
            source.push(Message::Flush("hi".into())).unwrap();

            assert_eq!(sink.read().unwrap(), Message::Data(2048));
            assert_eq!(sink.read().unwrap(), Message::Flush("hi".into()));
        }
        println!("Elapsed time: {:.2?}", before.elapsed());
    }

    #[test]
    fn close_propagates_through_push_when_input_closed() {
        let line = make_line(MockLine::new());
        let mut source = Source::new(&line).unwrap();
        let mut sink = Sink::new(line).unwrap();

        // Push some data
        source.push(Message::Data(1)).unwrap();

        // Read it
        assert_eq!(sink.read().unwrap(), Message::Data(2));

        // Close the source (input edge)
        source.close().unwrap();

        // Next read should return Closed because close propagated through the line
        let result = sink.read();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn close_propagates_through_work_chain() {
        let line_1 = make_line(MockLine::new());
        let mut line_2 = make_line(MockLine::new());

        let mut source = Source::new(&line_1).unwrap();
        make_bidi(line_1, &mut line_2).unwrap();
        let mut sink = Sink::<usize>::new(line_2).unwrap();

        // Push some data through the chain
        source.push(Message::Data(1)).unwrap();
        assert_eq!(sink.read().unwrap(), Message::Data(4)); // 1*2=2, 2*2=4

        // Close the source (input to first line)
        source.close().unwrap();

        // Next read should return Closed because close propagated through both lines
        let result = sink.read();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }
}
