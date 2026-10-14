//! [LineTrait] and default implementation for running [LineRoutine].
use crate::closed;
use crate::edge::fanout::Fanout;
use crate::edge::sync::Receiver;
use crate::error::{Error, ErrorKind};
use crate::graph::marker::Connection;
use crate::graph::{Add, Get, Outputs};
use crate::message::Message;
use crate::node::line::routine::LineRoutine;
use crate::node::line::work::builder::LineBuilder;
use crate::node::routine::{Flush as _, Next as _, Send as _};
use crate::node::work::Incoming;
use crate::signal::Origin;
use crate::thread::ThreadId;
use crate::work::Sink;
use crate::work::{Workable, work_each};
use std::any::type_name;
use std::marker::PhantomData;

// The contract of a `Sync` node forming a line.
/// [`LineTrait`] describes the contract of a line node.
///
/// - The node is [Workable]
/// - We can [Add] a [Workable] to it.
/// - We can [Add] [Sink] to it, outbound connections to send data.
/// - We can [Get] a [Sink] from it. Inbound connection to recieve data.
///
/// It forms the basis if a node that recieves one input stream of data and
/// produces one output stream of data.
///
/// Every node with these capabilities is a [LineTrait]; there is nothing to implement. Areamy
/// provides a default implementation [Line], but users are encouraged to write their own node
/// whenever [Line] does not suit their needs.
pub trait LineTrait<'params, In, Out, Signal>:
    // We can work on the line to produce output.
    Workable
    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params>
    // We can add edges it should push into.
    + Add<dyn Sink<DataType = Out, SignalType = Signal> + Send + Sync + 'params>
    // We can retrieve its edge for others to push into.
    + Get<dyn Sink<DataType = In, SignalType = Signal> + Send + Sync + 'params>
where
    Signal: Origin,
{
}

impl<'params, In, Out, Signal, LineType> LineTrait<'params, In, Out, Signal> for LineType
where
    Signal: Origin,
    LineType: Workable
        + Add<dyn Workable<ThreadId = <LineType as Workable>::ThreadId> + 'params>
        + Add<dyn Sink<DataType = Out, SignalType = Signal> + Send + Sync + 'params>
        + Get<dyn Sink<DataType = In, SignalType = Signal> + Send + Sync + 'params>,
{
}

/// The types a [Line] is built from. Bounds live here once, so impls take a single `T: Types`.
pub trait Types {
    type In: Send + Sync;
    type Out: Clone + Send + Sync;
    type Signal: Origin + Clone + Send + Sync;
    type ThreadId: ThreadId;
    type Routine: LineRoutine<Self::In, Self::Out>;
}

/// [Types] from its parts, so [Line::of] infers them from the routine and the wiring.
pub struct Node<In, Out, SignalType, ThreadIdType, RoutineType>(
    PhantomData<fn() -> (In, Out, SignalType, ThreadIdType, RoutineType)>,
);

impl<In, Out, SignalType, ThreadIdType, RoutineType> Types
    for Node<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
{
    type In = In;
    type Out = Out;
    type Signal = SignalType;
    type ThreadId = ThreadIdType;
    type Routine = RoutineType;
}

/// [`Line`] is a default implementation of [LineTrait]. See [Line::work]
/// for implementation details on how it works.
///
/// If the implementation is not suitable for a usecase then any node with
/// the [LineTrait] capabilities can be used as a drop-in replacement for [Line].
pub struct Line<'params, T: Types> {
    /// Worker or `Coroutine` associated with the current node.
    pub worker: T::Routine,

    /// Parent nodes that we can schedule to work.
    pub workers: Vec<Box<dyn Workable<ThreadId = T::ThreadId> + 'params>>,

    /// Edges that we push output into. A refused message waits here, and nothing new leaves the
    /// routine until it is delivered.
    pub outputs: Fanout<'params, T::Out, T::Signal>,

    /// Input to our current node that parents will push into.
    pub input: Receiver<T::In, T::Signal>,

    /// A Flush or Marker held until the routine is drained; outputs go out one per round.
    pub(super) pending: Option<Message<T::Out, T::Signal>>,
}

/// Mark our Line as a possible connection in a graph. It's a connection
/// because it is `Workable`.
impl<T: Types> Connection for Line<'_, T> {}

impl<T: Types> Workable for Line<'_, T> {
    /// One message per round into [Line::outputs]: finish a suspended push, else the next
    /// routine output, pending signal or input.
    fn work(&mut self) -> Result<(), Error> {
        let Line {
            worker,
            workers,
            outputs,
            input,
            pending,
        } = self;

        // Finish last round's push, or push one new message. Return even when the resume
        // completes: carrying on could block in `wait_front` after pushing.
        let Some(ready) = outputs.ready() else {
            // Complete or still suspended, this round is done; the fan-out remembers which.
            let _ = outputs.resume(type_name::<T::Routine>())?;
            return Ok(());
        };

        match next_message::<T>(worker, workers, input, pending)? {
            Incoming::Message(message) => {
                // A refused message stays in the fan-out; the next round resumes it.
                let _ = ready.push(message)?;
                Ok(())
            }
            Incoming::Closed => {
                // Close the outputs too, so children see it. Other errors, the routine's
                // `Closed` included, leave them to close on drop.
                let _ = outputs.close();
                Err(closed!())
            }
        }
    }

    type ThreadId = T::ThreadId;
}

impl<'params, In, Out, SignalType, ThreadIdType, RoutineType>
    Line<'params, Node<In, Out, SignalType, ThreadIdType, RoutineType>>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
{
    /// Create a [Line] with routine [LineRoutine]. Wire it with [crate::Push],
    /// [crate::work::Bidi] and [crate::work::Schedule]; its thread is inferred from them.
    ///
    /// * `worker` - A [LineRoutine] that will transform data in this node.
    pub fn of(worker: RoutineType) -> Self {
        Self::builder().build(worker)
    }

    /// Options for a line, such as [LineBuilder::bounded]; finish with [LineBuilder::build].
    pub fn builder() -> LineBuilder<'params, Node<In, Out, SignalType, ThreadIdType, RoutineType>> {
        LineBuilder::new()
    }
}

/// The next message for a [Line] to push: routine output first, then a pending signal (so a
/// Flush goes out behind everything its flush produced), then new input. Parents are scheduled
/// only once input polled empty: its own input goes out first, and a parent refused by this
/// input always finds progress (`Full`) when re-entered.
fn next_message<T: Types>(
    worker: &mut T::Routine,
    workers: &mut Vec<Box<dyn Workable<ThreadId = T::ThreadId> + '_>>,
    input: &Receiver<T::In, T::Signal>,
    pending: &mut Option<Message<T::Out, T::Signal>>,
) -> Result<Incoming<Message<T::Out, T::Signal>>, Error> {
    loop {
        if let Some(output) = worker.next()? {
            return Ok(Incoming::Message(Message::Data(output)));
        }
        if let Some(signal) = pending.take() {
            return Ok(Incoming::Message(signal));
        }
        match input.poll() {
            Ok(Some(Message::Data(data))) => worker.send(data)?,
            Ok(Some(Message::Flush(origin))) => {
                worker.flush()?;
                *pending = Some(Message::Flush(origin));
            }
            Ok(Some(Message::Marker(origin))) => *pending = Some(Message::Marker(origin)),
            Ok(None) => {
                // No parents left: block on the edge. Closed once it can never fill.
                if workers.is_empty() {
                    match input.wait_front() {
                        Err(error) if matches!(error.kind, ErrorKind::Closed) => {
                            return Ok(Incoming::Closed);
                        }
                        waited => waited?,
                    }
                }
                // Work each parent once, dropping finished ones. The edge closes itself once
                // every producer is gone.
                work_each(workers)?;
            }
            Err(error) if matches!(error.kind, ErrorKind::Closed) => return Ok(Incoming::Closed),
            Err(error) => return Err(error),
        }
    }
}

/// Get a [Sink] for this node's input edge.
impl<'params, T: Types>
    Get<dyn Sink<DataType = T::In, SignalType = T::Signal> + Send + Sync + 'params>
    for Line<'params, T>
where
    T::In: 'static,
    T::Signal: 'static,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = T::In, SignalType = T::Signal> + Send + Sync + 'params>,
        Error,
    > {
        Get::get(&self.input)
    }
}

/// Implement the [Add] constructor for inbound [Workable] edge.
/// This allows users to add [Workable] edges to this node such that it can schedule them.
impl<'params, T: Types> Add<dyn Workable<ThreadId = T::ThreadId> + 'params> for Line<'params, T> {
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = T::ThreadId> + 'params>,
    ) -> Result<(), Error> {
        self.workers.push(workable);
        Ok(())
    }
}

/// Implement the [Add] constructor for output [Sink] edge.
/// This allows users to add [Sink] edges to this node such that it can push data
/// into them when scheduled, and close them on shutdown.
impl<'params, T: Types>
    Add<dyn Sink<DataType = T::Out, SignalType = T::Signal> + Send + Sync + 'params>
    for Line<'params, T>
{
    fn add(
        &mut self,
        closeable: Box<dyn Sink<DataType = T::Out, SignalType = T::Signal> + Send + Sync + 'params>,
    ) -> Result<(), Error> {
        self.outputs.add(closeable);
        Ok(())
    }
}

impl<'params, T: Types> Outputs for Line<'params, T> {
    type Sink = dyn Sink<DataType = T::Out, SignalType = T::Signal> + Send + Sync + 'params;
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::DefaultThread;
    use crate::edge::sync::tests::{join_within, wait_until};
    use crate::graph::marker::Multiplicity;
    use crate::graph::{Closeable, Pushable};
    use crate::node::line::routine::tests::{AccMockLine, ClosesAfterOne, MockLine, MockWaitLine};
    use crate::signal::Trackable;
    use crate::work::{self, Reader, Writer, tee};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Instant;

    type TestSignal = Trackable<&'static str>;
    type TestLine<'params, R> = Line<'params, Node<usize, usize, TestSignal, DefaultThread, R>>;

    /// Counts the `work()` calls of the parent it wraps.
    struct Counting<W> {
        inner: W,
        calls: Arc<AtomicUsize>,
    }

    impl<W> Connection for Counting<W> {}

    impl<W: Workable> Workable for Counting<W> {
        type ThreadId = W::ThreadId;
        fn work(&mut self) -> Result<(), Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.work()
        }
    }

    impl<T, M, W> Add<T, M> for Counting<W>
    where
        T: Connection + ?Sized,
        M: Multiplicity,
        W: Add<T, M>,
    {
        fn add(&mut self, connection: Box<T>) -> Result<(), Error> {
            self.inner.add(connection)
        }
    }

    /// A pass-through line whose input holds at most one message.
    fn bound_one_line<'params>() -> TestLine<'params, MockWaitLine> {
        Line::builder()
            .bounded(NonZeroUsize::MIN)
            .build(MockWaitLine::new(1))
    }

    /// [Line], and a shared `Arc<Mutex<Line>>`, meet the [LineTrait] contract.
    #[test]
    fn line_meets_line_trait() {
        fn contract<'params, L: LineTrait<'params, usize, usize, TestSignal>>(_: &L) {}
        let line: TestLine<MockLine> = Line::of(MockLine::new());
        contract(&line);
        contract(&Arc::new(Mutex::new(line)));
    }

    #[test]
    fn parked_parent_delivers_one_message_per_work() {
        // Holds 5 inputs until the Flush, then releases them into a bound-1 owner edge.
        let parent = Line::of(MockWaitLine::new(10));
        let mut writer = Writer::new(&parent).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let counting = Counting {
            inner: parent,
            calls: calls.clone(),
        };
        let mut child = bound_one_line();
        work::Bidi::connect(counting, &mut child).unwrap();
        let mut reader = Reader::new(child).unwrap();

        for value in 1..=5 {
            writer.push(Message::Data(value)).unwrap();
        }
        writer.push(Message::Flush("f".into())).unwrap();

        // On a thread: carrying on after a completed resume would leave the parentless parent
        // blocked in `wait_front` (the writer stays open).
        let reading = thread::spawn(move || (0..6).map(|_| reader.read().unwrap()).collect());
        let got: Vec<Message<usize, TestSignal>> = join_within(reading);
        let mut expected: Vec<_> = (1..=5).map(Message::Data).collect();
        expected.push(Message::Flush("f".into()));
        assert_eq!(got, expected);
        // One work per delivered message: parking never spins.
        assert_eq!(calls.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn close_while_parked_comes_after_the_parked_messages() {
        let parent = Line::of(MockWaitLine::new(10));
        let mut writer = Writer::new(&parent).unwrap();
        let mut child = bound_one_line();
        work::Bidi::connect(parent, &mut child).unwrap();
        let mut reader = Reader::new(child).unwrap();

        for value in 1..=5 {
            writer.push(Message::Data(value)).unwrap();
        }
        writer.push(Message::Flush("f".into())).unwrap();
        writer.close().unwrap();

        for value in 1..=5 {
            assert_eq!(reader.read().unwrap(), Message::Data(value));
        }
        assert_eq!(reader.read().unwrap(), Message::Flush("f".into()));
        assert!(matches!(reader.read().unwrap_err().kind, ErrorKind::Closed));
    }

    /// True if `values` is `start, start + 1, ...`: a source's messages arrived in order, none
    /// lost.
    fn counts_up_from(values: &[usize], start: usize) -> bool {
        values
            .iter()
            .enumerate()
            .all(|(i, value)| *value == start + i)
    }

    #[test]
    fn owner_edge_refilled_from_another_thread_never_blocks_the_parent() {
        const N: usize = 200;
        const DIRECT: usize = 10_000;
        let parent = Line::of(MockWaitLine::new(1));
        let mut to_parent = Writer::new(&parent).unwrap();
        let mut child = bound_one_line();
        let mut direct = Writer::new(&child).unwrap();
        work::Bidi::connect(parent, &mut child).unwrap();
        let mut reader = Reader::new(child).unwrap();

        // More than will be read: a parent that runs dry blocks in `wait_front` (a paradigm
        // limit), which would stall the read for reasons unrelated to this test.
        for value in 0..2 * N {
            to_parent.push(Message::Data(value)).unwrap();
        }
        // Races the parent for every slot the child frees. Stops once the child is gone.
        let refiller = thread::spawn(move || {
            for value in DIRECT.. {
                if direct.push(Message::Data(value)).is_err() {
                    break;
                }
            }
        });
        // A parent blocking on its owner's full input would deadlock here. Dropping the reader
        // at the end closes the child's input, which releases the refiller.
        let reading = thread::spawn(move || {
            (0..N)
                .map(|_| reader.read().unwrap().data().unwrap())
                .collect::<Vec<usize>>()
        });
        let got = join_within(reading);
        join_within(refiller);

        let (from_parent, from_direct): (Vec<usize>, Vec<usize>) =
            got.into_iter().partition(|value| *value < DIRECT);
        assert!(counts_up_from(&from_parent, 0), "{from_parent:?}");
        assert!(counts_up_from(&from_direct, DIRECT), "{from_direct:?}");
    }

    #[test]
    fn own_input_goes_out_before_parents_are_worked() {
        // A parentless parent with an open, silent writer: working it would block in
        // `wait_front`.
        let parent = Line::of(MockWaitLine::new(1));
        let _silent = Writer::new(&parent).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let counting = Counting {
            inner: parent,
            calls: calls.clone(),
        };
        let mut child: TestLine<MockWaitLine> = Line::of(MockWaitLine::new(1));
        let mut direct = Writer::new(&child).unwrap();
        work::Bidi::connect(counting, &mut child).unwrap();
        let mut reader = Reader::new(child).unwrap();

        for value in 0..3 {
            direct.push(Message::Data(value)).unwrap();
        }
        let reading = thread::spawn(move || {
            (0..3)
                .map(|_| reader.read().unwrap().data().unwrap())
                .collect::<Vec<usize>>()
        });
        assert_eq!(join_within(reading), vec![0, 1, 2]);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn routine_closed_in_one_parent_keeps_the_siblings_data() {
        let closing = Line::of(ClosesAfterOne::new());
        let open = Line::of(MockWaitLine::new(1));
        let mut to_closing = Writer::new(&closing).unwrap();
        let mut to_open = Writer::new(&open).unwrap();
        to_closing.push(Message::Data(1000)).unwrap();
        for value in 0..5 {
            to_open.push(Message::Data(value)).unwrap();
        }
        let mut child: TestLine<MockWaitLine> = Line::of(MockWaitLine::new(1));
        work::Bidi::connect(closing, &mut child).unwrap();
        work::Bidi::connect(open, &mut child).unwrap();
        let mut reader = Reader::new(child).unwrap();

        // The closing routine ends its own line only: the shared input stays open for `open`.
        let reading = thread::spawn(move || {
            (0..6)
                .map(|_| reader.read().unwrap().data().unwrap())
                .collect::<Vec<usize>>()
        });
        let mut got = join_within(reading);
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 3, 4, 1000]);
    }

    #[test]
    fn signals_with_no_outputs_are_consumed() {
        let mut line: TestLine<MockWaitLine> = Line::of(MockWaitLine::new(1));
        let mut writer = Writer::new(&line).unwrap();
        writer.push(Message::Marker("m".into())).unwrap();
        writer.push(Message::Flush("f".into())).unwrap();
        writer.push(Message::Data(1)).unwrap();

        for _ in 0..3 {
            line.work().unwrap();
        }
        assert!(line.input.is_empty().unwrap());
    }

    #[test]
    fn full_side_path_yields_once_then_blocks_until_a_pop() {
        let mut line: TestLine<MockWaitLine> = Line::of(MockWaitLine::new(1));
        let side = Receiver::<usize, TestSignal>::bounded(NonZeroUsize::MIN);
        let edge: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Box::new(side.sender());
        Add::add(&mut line, edge).unwrap();
        let mut writer = Writer::new(&line).unwrap();
        for value in 0..3 {
            writer.push(Message::Data(value)).unwrap();
        }

        line.work().unwrap();
        // Refused with `Full`: the line yields instead of blocking.
        line.work().unwrap();
        assert_eq!(line.outputs.suspended(), Some(0));

        // Nothing popped since: `Stuck`, so this round blocks until the consumer pops.
        let working = thread::spawn(move || line.work().map(|()| line));
        wait_until(|| side.producers_waiting() == 1);
        assert_eq!(side.poll().unwrap(), Some(Message::Data(0)));
        let line = join_within(working).unwrap();
        assert_eq!(line.outputs.suspended(), None);
        assert_eq!(side.poll().unwrap(), Some(Message::Data(1)));
    }

    #[test]
    fn parents_take_turns_into_a_bounded_input() {
        const N: usize = 20;
        let first = Line::of(MockWaitLine::new(1));
        let second = Line::of(MockWaitLine::new(1));
        let mut to_first = Writer::new(&first).unwrap();
        let mut to_second = Writer::new(&second).unwrap();
        for value in 0..N {
            to_first.push(Message::Data(value)).unwrap();
            to_second.push(Message::Data(N + value)).unwrap();
        }
        let mut child = bound_one_line();
        work::Bidi::connect(first, &mut child).unwrap();
        work::Bidi::connect(second, &mut child).unwrap();
        let mut reader = Reader::new(child).unwrap();

        // Reads N of the 2N: neither parent runs dry, since a dry parent blocks in `wait_front`
        // (a paradigm limit) and would stall its sibling.
        let reading = thread::spawn(move || {
            (0..N)
                .map(|_| reader.read().unwrap().data().unwrap())
                .collect::<Vec<usize>>()
        });
        let got = join_within(reading);

        // Both progress from the start; a fixed order would deliver only `first` here.
        let from_second = got.iter().filter(|value| **value >= N).count();
        assert!(from_second >= N / 4, "{got:?}");
    }

    #[test]
    fn line_accumulating_node_works() {
        let line = Line::of(AccMockLine::new());
        let mut writer = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(vec![1, 2]));
    }

    #[test]
    fn line_wait_node_mark_waits() {
        let line = Line::of(MockWaitLine::new(4));
        let mut writer = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Data(3)).unwrap();

        // Send a marker
        writer.push(Message::Marker("no_output".into())).unwrap();
        // We get marker because no output is ready.
        assert_eq!(reader.read().unwrap(), Message::Marker("no_output".into()));

        // Send data so that node releases all data.
        writer.push(Message::Data(4)).unwrap();

        // Send marker.
        writer.push(Message::Marker("output!".into())).unwrap();

        // Now we get all data in order and marker last.
        assert_eq!(reader.read().unwrap(), Message::Data(1));
        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(3));
        assert_eq!(reader.read().unwrap(), Message::Data(4));
        // The marker will arrive last!
        assert_eq!(reader.read().unwrap(), Message::Marker("output!".into()));
    }

    #[test]
    fn line_wait_node_flush_waits() {
        let line = Line::of(MockWaitLine::new(4));
        let mut writer = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Data(3)).unwrap();
        writer.push(Message::Flush("force_output".into())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(1));
        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(3));
        // The flush will arrive last!
        assert_eq!(
            reader.read().unwrap(),
            Message::Flush("force_output".into())
        );
    }

    #[test]
    fn line_basic_run() {
        let line = Line::of(MockLine::new());
        let mut writer = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(6));

        // Reset processing
        writer.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));

        writer.push(Message::Data(2)).unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn line_can_mark() {
        let line = Line::of(MockLine::new());
        let mut writer = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        // One data
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Marker("hi".into())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Marker("hi".into()));
    }

    #[test]
    fn line_can_flush() {
        let line = Line::of(MockLine::new());
        let mut writer = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        // One data
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Flush("hi".into())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));
    }

    #[test]
    fn line_can_be_stacked() {
        let line_1 = Line::of(MockLine::new());
        let mut line_2 = Line::of(MockLine::new());

        let mut writer = Writer::new(&line_1).unwrap();
        work::Bidi::connect(line_1, &mut line_2).unwrap();

        let mut reader = Reader::new(line_2).unwrap();

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(reader.read().unwrap(), Message::Data(16));

        // Reset processing
        writer.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));

        writer.push(Message::Data(2)).unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(8));
    }

    #[test]
    fn line_can_be_stacked_with_type_hints() {
        let line_1 = Line::of(MockLine::new());
        let mut line_2 = Line::of(MockLine::new());

        let mut writer = Writer::new(&line_1).unwrap();

        // This typehint is not needed as exemplified by other tests
        // but it helps readability to be explicit when building
        // the graph.
        work::Bidi::<usize>::connect(line_1, &mut line_2).unwrap();

        let mut reader = Reader::<usize>::new(line_2).unwrap();

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(reader.read().unwrap(), Message::Data(16));

        // Reset processing
        writer.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));

        writer.push(Message::Data(2)).unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(8));
    }

    #[test]
    fn line_can_tee() {
        let mut line = Line::of(MockLine::new());

        let mut writer = Writer::new(&line).unwrap();
        let mut reader_1 = tee::Reader::new(&mut line).unwrap();
        let mut reader_2 = tee::Reader::new(&mut line).unwrap();

        let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = Box::new(line);

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        workable.work().unwrap();
        workable.work().unwrap();

        assert_eq!(reader_1.read().unwrap(), Message::Data(2));
        assert_eq!(reader_1.read().unwrap(), Message::Data(6));

        assert_eq!(reader_2.read().unwrap(), Message::Data(2));
        assert_eq!(reader_2.read().unwrap(), Message::Data(6));

        // Reset processing
        writer.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        workable.work().unwrap();
        assert_eq!(reader_1.read().unwrap(), Message::Flush("hi".into()));
        assert_eq!(reader_2.read().unwrap(), Message::Flush("hi".into()));

        writer.push(Message::Data(2)).unwrap();
        workable.work().unwrap();
        assert_eq!(reader_1.read().unwrap(), Message::Data(4));
        assert_eq!(reader_2.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn line_can_merge() {
        let line = Line::of(MockLine::new());

        let mut writer_1 = Writer::new(&line).unwrap();
        let mut writer_2 = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        // Add one flush
        writer_1.push(Message::Data(1)).unwrap();
        writer_2.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(6));

        // Note for merging the markers have to have different
        // IDs otherwise they'll be treated as duplicates
        // and stopped.
        writer_2.push(Message::Marker("writer_2".into())).unwrap();
        writer_1.push(Message::Flush("writer_1".into())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Marker("writer_2".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("writer_1".into()));

        writer_1.push(Message::Data(2)).unwrap();
        writer_2.push(Message::Data(1)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(reader.read().unwrap(), Message::Data(6));
    }

    #[ignore]
    #[test]
    fn line_basic_many_stack_benchmark() {
        let line_0 = Line::of(MockLine::new());

        let mut writer = Writer::new(&line_0).unwrap();

        let mut line_1 = Line::of(MockLine::new());
        let mut line_2 = Line::of(MockLine::new());
        let mut line_3 = Line::of(MockLine::new());
        let mut line_4 = Line::of(MockLine::new());
        let mut line_5 = Line::of(MockLine::new());
        let mut line_6 = Line::of(MockLine::new());
        let mut line_7 = Line::of(MockLine::new());
        let mut line_8 = Line::of(MockLine::new());
        let mut line_9 = Line::of(MockLine::new());
        let mut line_10 = Line::of(MockLine::new());

        work::Bidi::connect(line_0, &mut line_1).unwrap();
        work::Bidi::connect(line_1, &mut line_2).unwrap();
        work::Bidi::connect(line_2, &mut line_3).unwrap();
        work::Bidi::connect(line_3, &mut line_4).unwrap();
        work::Bidi::connect(line_4, &mut line_5).unwrap();
        work::Bidi::connect(line_5, &mut line_6).unwrap();
        work::Bidi::connect(line_6, &mut line_7).unwrap();
        work::Bidi::connect(line_7, &mut line_8).unwrap();
        work::Bidi::connect(line_8, &mut line_9).unwrap();
        work::Bidi::connect(line_9, &mut line_10).unwrap();

        let mut reader = Reader::<usize>::new(line_10).unwrap();

        let before = Instant::now();
        for _ in 0..10000 {
            writer.push(Message::Data(1)).unwrap();
            writer.push(Message::Flush("hi".into())).unwrap();

            assert_eq!(reader.read().unwrap(), Message::Data(2048));
            assert_eq!(reader.read().unwrap(), Message::Flush("hi".into()));
        }
        println!("Elapsed time: {:.2?}", before.elapsed());
    }

    #[test]
    fn close_propagates_through_push_when_input_closed() {
        let line = Line::of(MockLine::new());
        let mut writer = Writer::new(&line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        // Push some data
        writer.push(Message::Data(1)).unwrap();

        // Read it
        assert_eq!(reader.read().unwrap(), Message::Data(2));

        // Close the writer (input edge)
        writer.close().unwrap();

        // Next read should return Closed because close propagated through the line
        let result = reader.read();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn close_propagates_through_work_chain() {
        let line_1 = Line::of(MockLine::new());
        let mut line_2 = Line::of(MockLine::new());

        let mut writer = Writer::new(&line_1).unwrap();
        work::Bidi::connect(line_1, &mut line_2).unwrap();
        let mut reader = Reader::<usize>::new(line_2).unwrap();

        // Push some data through the chain
        writer.push(Message::Data(1)).unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(4)); // 1*2=2, 2*2=4

        // Close the writer (input to first line)
        writer.close().unwrap();

        // Next read should return Closed because close propagated through both lines
        let result = reader.read();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }
}
