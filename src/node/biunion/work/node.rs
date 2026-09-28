use crate::closed;
use crate::edge::fanout::Fanout;
use crate::error::{Error, ErrorKind};
use crate::graph::marker::{Connection, Multiplicity};
use crate::graph::{Add, Get, Pushable, Sink};
use crate::message::Message;
use crate::node::biunion::routine::BiunionRoutine;
use crate::node::biunion::work::builder::BiunionBuilder;
use crate::node::work::Incoming;
use crate::node::{biunion, routine};
use crate::signal::Origin;
use crate::thread::ThreadId;
use crate::work::multiedge::{self, Notify};
use crate::work::{Workable, work_each};
use std::any::type_name;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a biunion.
// it has two workable sources and inputs.
pub trait BiunionTrait<'params>:
    // We can work on the line to produce output.
    Workable
    // We can add edges it should push into.
    + Add<dyn Sink<DataType = Self::Out, SignalType = Self::Signal> + Send + Sync + 'params>

    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params, biunion::Left>
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params, biunion::Right>

    // We can retrieve pushable edges
    + Get<dyn Pushable<DataType = Self::Left, SignalType = Self::Signal> + 'params, biunion::Left>
    + Get<dyn Pushable<DataType = Self::Right, SignalType = Self::Signal> + 'params, biunion::Right>

    // We can retrieve Closeable for closing the input edges.
    + Get<dyn Sink<DataType = Self::Left, SignalType = Self::Signal> + Send + Sync + 'params, biunion::Left>
    + Get<dyn Sink<DataType = Self::Right, SignalType = Self::Signal> + Send + Sync + 'params, biunion::Right>
{
    // The input data going into the line.
    type Left:  Send + Sync + 'static;
    type Right:  Send + Sync + 'static;
    // The output data leaving it..
    type Out: Clone + Send + Sync;
    // The signal type used in the graph.
    type Signal: Origin + Clone + 'static;
    // The coroutine associated with this node.
    type BiunionRoutine: BiunionRoutine<Self::Left, Self::Right, Self::Out>;

}

// A `Send+Sync` variant of our node. Looks how neatly all the
// `AddWorkable` and `GetPushable` parameters are automatically derived
// for this since those traits are generic over our builders :))
impl<'params, BiunionType: BiunionTrait<'params>> BiunionTrait<'params>
    for Arc<Mutex<BiunionType>>
{
    type Left = BiunionType::Left;
    type Right = BiunionType::Right;
    type Out = BiunionType::Out;
    type Signal = BiunionType::Signal;
    type BiunionRoutine = BiunionType::BiunionRoutine;
}

/// Parent workables grouped by biunion side.
pub struct Worker<'params, ThreadIdType: ThreadId> {
    pub left: Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,
    pub right: Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,
}

impl<'params, ThreadIdType: ThreadId> Default for Worker<'params, ThreadIdType> {
    fn default() -> Self {
        Self {
            left: Vec::new(),
            right: Vec::new(),
        }
    }
}

/// Input edges grouped by biunion side.
pub struct Input<Left, Right, SignalType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    pub left: multiedge::Receiver<Left, SignalType>,
    pub right: multiedge::Receiver<Right, SignalType>,
    notify: Arc<Notify>,
}

impl<Left, Right, SignalType> Input<Left, Right, SignalType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    /// Block until either side has data; `Closed` when neither can.
    fn wait_any(&self) -> Result<(), Error> {
        self.notify.wait_any(&[&self.left, &self.right])
    }
}

impl<Left, Right, SignalType> Input<Left, Right, SignalType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    /// Both sides share one [Notify]; each has its own bound (`None` is unbounded).
    pub(super) fn new(left: Option<NonZeroUsize>, right: Option<NonZeroUsize>) -> Self {
        let notify = Notify::new();
        Self {
            left: multiedge::Receiver::new(notify.clone(), left),
            right: multiedge::Receiver::new(notify.clone(), right),
            notify,
        }
    }
}

pub struct Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    /// The coroutine of this node.
    pub routine: RoutineType,

    /// Parent workables, grouped by side.
    pub worker: Worker<'params, ThreadIdType>,

    /// Edges that we push output into. A refused message waits here, and nothing new leaves the
    /// routine until it is delivered.
    pub outputs: Fanout<'params, Out, SignalType>,

    /// Input edges, grouped by side.
    pub input: Input<Left, Right, SignalType>,

    /// A Flush or Marker held until the routine is drained; outputs go out one per round.
    pub(super) pending: Option<Message<Out, SignalType>>,
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> Connection
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> Workable
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    /// One message per round into [Biunion::outputs]: finish a suspended push, else the next
    /// routine output, pending signal or input.
    fn work(&mut self) -> Result<(), Error> {
        let Biunion {
            routine,
            worker,
            outputs,
            input,
            pending,
        } = self;

        // Finish last round's push, or push one new message. Return even when the resume
        // completes: carrying on could block in `wait_any` after pushing.
        let Some(ready) = outputs.ready() else {
            // Complete or still suspended, this round is done; the fan-out remembers which.
            let _ = outputs.resume(type_name::<RoutineType>())?;
            return Ok(());
        };

        match next_message(routine, worker, input, pending)? {
            Incoming::Message(message) => {
                // A refused message stays in the fan-out; the next round resumes it.
                let _ = ready.push(message)?;
                Ok(())
            }
            Incoming::Closed => {
                // An input closed: close the outputs too, so children see it. Other errors, the
                // routine's `Closed` included, leave them to close on drop.
                let _ = outputs.close();
                Err(closed!())
            }
        }
    }

    type ThreadId = ThreadIdType;
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    pub fn of(routine: RoutineType) -> Self {
        Self::builder().build(routine)
    }

    /// Options for a biunion, such as [BiunionBuilder::bounded]; finish with
    /// [BiunionBuilder::build].
    pub fn builder()
    -> BiunionBuilder<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> {
        BiunionBuilder::new()
    }
}

/// The next message for a [Biunion] to push: routine output first, then a pending signal (so a
/// Flush goes out behind everything its flush produced), then new input, left before right;
/// closed once either input has. Parents are scheduled only once both inputs polled empty: own
/// input goes out first, and a parent refused by an input always finds progress (`Full`) when
/// re-entered.
fn next_message<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>(
    routine: &mut RoutineType,
    worker: &mut Worker<'params, ThreadIdType>,
    input: &Input<Left, Right, SignalType>,
    pending: &mut Option<Message<Out, SignalType>>,
) -> Result<Incoming<Message<Out, SignalType>>, Error>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    loop {
        if let Some(output) = routine.next()? {
            return Ok(Incoming::Message(Message::Data(output)));
        }
        if let Some(signal) = pending.take() {
            return Ok(Incoming::Message(signal));
        }
        match input.left.poll() {
            Ok(Some(message)) => {
                accept(routine, pending, message, biunion::Left)?;
                continue;
            }
            Ok(None) => {}
            Err(error) if matches!(error.kind, ErrorKind::Closed) => return Ok(Incoming::Closed),
            Err(error) => return Err(error),
        }
        match input.right.poll() {
            Ok(Some(message)) => {
                accept(routine, pending, message, biunion::Right)?;
                continue;
            }
            Ok(None) => {}
            Err(error) if matches!(error.kind, ErrorKind::Closed) => return Ok(Incoming::Closed),
            Err(error) => return Err(error),
        }
        // No parents left: block until either edge has data. Closed once neither can.
        if worker.left.is_empty() && worker.right.is_empty() {
            match input.wait_any() {
                Err(error) if matches!(error.kind, ErrorKind::Closed) => {
                    return Ok(Incoming::Closed);
                }
                waited => waited?,
            }
        }
        // Work each side's parents once, dropping finished ones.
        work_each(&mut worker.left)?;
        work_each(&mut worker.right)?;
    }
}

/// Take one input message from `_side`: data goes to the routine, a signal waits in `pending`
/// until the routine is drained. The side is passed by value so it's inferred: a routine sends
/// the same data type on both sides, so the input type alone can't pick the `Send` impl.
fn accept<In, Side, Out, SignalType, RoutineType>(
    routine: &mut RoutineType,
    pending: &mut Option<Message<Out, SignalType>>,
    message: Message<In, SignalType>,
    _side: Side,
) -> Result<(), Error>
where
    Side: Multiplicity,
    SignalType: Origin,
    RoutineType: routine::Send<In, Side> + routine::Flush,
{
    match message {
        Message::Data(data) => routine.send(data)?,
        Message::Flush(origin) => {
            routine.flush()?;
            *pending = Some(Message::Flush(origin));
        }
        Message::Marker(origin) => *pending = Some(Message::Marker(origin)),
    }
    Ok(())
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> BiunionTrait<'params>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    type Left = Left;
    type Right = Right;
    type Out = Out;
    type Signal = SignalType;
    type BiunionRoutine = RoutineType;
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = Left, SignalType = SignalType> + 'params, biunion::Left>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = Left, SignalType = SignalType> + 'params>, Error> {
        Get::get(&self.input.left)
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = Right, SignalType = SignalType> + 'params, biunion::Right>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = Right, SignalType = SignalType> + 'params>, Error> {
        Get::get(&self.input.right)
    }
}

/// Get a [Sink] for the left input edge.
impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Sink<DataType = Left, SignalType = SignalType> + Send + Sync + 'params, biunion::Left>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = Left, SignalType = SignalType> + Send + Sync + 'params>,
        Error,
    > {
        Get::get(&self.input.left)
    }
}

/// Get a [Sink] for the right input edge.
impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Sink<DataType = Right, SignalType = SignalType> + Send + Sync + 'params, biunion::Right>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = Right, SignalType = SignalType> + Send + Sync + 'params>,
        Error,
    > {
        Get::get(&self.input.right)
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType> + 'params, biunion::Left>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    ) -> Result<(), Error> {
        self.worker.left.push(workable);
        Ok(())
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType> + 'params, biunion::Right>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    ) -> Result<(), Error> {
        self.worker.right.push(workable);
        Ok(())
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Sink<DataType = Out, SignalType = SignalType> + Send + Sync + 'params>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn add(
        &mut self,
        closeable: Box<dyn Sink<DataType = Out, SignalType = SignalType> + Send + Sync + 'params>,
    ) -> Result<(), Error> {
        self.outputs.add(closeable);
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::edge::sync::Receiver;
    use crate::edge::sync::tests::{join_within, wait_until};
    use crate::graph::TryPush;
    use crate::node::biunion::routine::tests::{HoldBiunion, MockBiunion};
    use crate::node::line::routine::tests::MockWaitLine;
    use crate::work::{self, Line, Reader, Writer};
    use crate::{At, Push, Pushable};
    use crate::{DefaultThread, Trackable};
    use std::thread;

    type TestSignal = Trackable<&'static str>;
    type Hold<'params> =
        Biunion<'params, usize, usize, usize, TestSignal, DefaultThread, HoldBiunion>;

    #[test]
    fn marker_passes_through() {
        let mut biunion: Hold = Biunion::of(HoldBiunion::new(1));
        let mut left = Writer::new(&biunion.at::<biunion::Left>()).unwrap();
        let mut right = Writer::new(&biunion.at::<biunion::Right>()).unwrap();
        let mut reader = Reader::new(biunion).unwrap();

        left.push(Message::Marker("l".into())).unwrap();
        right.push(Message::Marker("r".into())).unwrap();
        // On a thread: a dropped Marker would leave the read waiting forever.
        let reading = thread::spawn(move || [reader.read().unwrap(), reader.read().unwrap()]);
        assert_eq!(
            join_within(reading),
            [Message::Marker("l".into()), Message::Marker("r".into())]
        );
    }

    #[test]
    fn closed_output_ends_the_round_with_closed() {
        let mut biunion: Hold = Biunion::of(HoldBiunion::new(1));
        let output = Receiver::<usize, TestSignal>::new();
        let edge: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Box::new(output.sender());
        Add::add(&mut biunion, edge).unwrap();
        let mut left = Writer::new(&biunion.at::<biunion::Left>()).unwrap();
        left.push(Message::Data(1)).unwrap();

        // A push error ends the round like any other.
        output.close().unwrap();
        assert!(matches!(
            biunion.work().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn flush_goes_out_after_everything_it_released() {
        let mut biunion: Hold = Biunion::of(HoldBiunion::new(10));
        let mut left = Writer::new(&biunion.at::<biunion::Left>()).unwrap();
        let mut reader = Reader::new(biunion).unwrap();

        left.push(Message::Data(1)).unwrap();
        left.push(Message::Data(2)).unwrap();
        left.push(Message::Flush("f".into())).unwrap();

        // The flush releases both held messages; the Flush must not overtake the second.
        assert_eq!(reader.read().unwrap(), Message::Data(1));
        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Flush("f".into()));
    }

    /// Pushes until refused; returns how many the side took.
    fn capacity(
        side: &mut Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync>,
    ) -> usize {
        (0..10)
            .take_while(|value| side.try_push(Message::Data(*value)).unwrap() == TryPush::Pushed)
            .count()
    }

    #[test]
    fn each_side_takes_its_own_bound() {
        let mut biunion: Hold = Biunion::builder()
            .bounded::<biunion::Left>(NonZeroUsize::new(1).unwrap())
            .bounded::<biunion::Right>(NonZeroUsize::new(2).unwrap())
            .build(HoldBiunion::new(1));
        let mut left = Get::get(&biunion.at::<biunion::Left>()).unwrap();
        let mut right = Get::get(&biunion.at::<biunion::Right>()).unwrap();
        assert_eq!(capacity(&mut left), 1);
        assert_eq!(capacity(&mut right), 2);

        // A side left out stays unbounded.
        let mut biunion: Hold = Biunion::builder()
            .bounded::<biunion::Left>(NonZeroUsize::MIN)
            .build(HoldBiunion::new(1));
        let mut right = Get::get(&biunion.at::<biunion::Right>()).unwrap();
        assert_eq!(capacity(&mut right), 10);
    }

    #[test]
    fn full_side_path_yields_once_then_blocks_until_a_pop() {
        let mut biunion: Hold = Biunion::of(HoldBiunion::new(1));
        let side = Receiver::<usize, TestSignal>::bounded(NonZeroUsize::MIN);
        let edge: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Box::new(side.sender());
        Add::add(&mut biunion, edge).unwrap();
        let mut left = Writer::new(&biunion.at::<biunion::Left>()).unwrap();
        for value in 0..3 {
            left.push(Message::Data(value)).unwrap();
        }

        biunion.work().unwrap();
        // Refused with `Full`: the biunion yields instead of blocking.
        biunion.work().unwrap();
        assert_eq!(biunion.outputs.suspended(), Some(0));

        // Nothing popped since: `Stuck`, so this round blocks until the consumer pops.
        let working = thread::spawn(move || biunion.work().map(|()| biunion));
        wait_until(|| side.producers_waiting() == 1);
        assert_eq!(side.poll().unwrap(), Some(Message::Data(0)));
        let biunion = join_within(working).unwrap();
        assert_eq!(biunion.outputs.suspended(), None);
        assert_eq!(side.poll().unwrap(), Some(Message::Data(1)));
    }

    #[test]
    fn two_parents_share_a_bounded_side() {
        const N: usize = 20;
        let first = Line::of(MockWaitLine::new(1));
        let second = Line::of(MockWaitLine::new(1));
        let mut to_first = Writer::new(&first).unwrap();
        let mut to_second = Writer::new(&second).unwrap();
        for value in 0..N {
            to_first.push(Message::Data(value)).unwrap();
            to_second.push(Message::Data(N + value)).unwrap();
        }
        let mut biunion: Hold = Biunion::builder()
            .bounded::<biunion::Left>(NonZeroUsize::MIN)
            .build(HoldBiunion::new(1));
        // Whoever pushes second into the bound-1 left side is refused (`Full`) and parks.
        work::Bidi::connect(first, &mut biunion.at::<biunion::Left>()).unwrap();
        work::Bidi::connect(second, &mut biunion.at::<biunion::Left>()).unwrap();
        let mut reader = Reader::new(biunion).unwrap();

        // Reads N of the 2N: a parent that runs dry blocks in `wait_front` (a paradigm limit).
        let reading = thread::spawn(move || {
            (0..N)
                .map(|_| reader.read().unwrap().data().unwrap())
                .collect::<Vec<usize>>()
        });
        let got = join_within(reading);

        // Both progressed, each in order.
        let (from_first, from_second): (Vec<usize>, Vec<usize>) =
            got.iter().partition(|value| **value < N);
        assert_eq!(from_first, (0..from_first.len()).collect::<Vec<_>>());
        assert_eq!(from_second, (N..N + from_second.len()).collect::<Vec<_>>());
        assert!(from_second.len() >= N / 4, "{got:?}");
    }

    #[test]
    fn run_biunion() {
        let mut biun = Biunion::of(MockBiunion::new());

        let mut left_writer = Writer::new(&biun.at::<biunion::Left>()).unwrap();
        let mut right_writer = Writer::new(&biun.at::<biunion::Right>()).unwrap();

        let mut reader = Reader::new(biun).unwrap();

        left_writer.push(Message::Data(1)).unwrap();
        right_writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(7));

        left_writer.push(Message::Flush("left".into())).unwrap();
        right_writer.push(Message::Flush("right".into())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Flush("left".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("right".into()));

        left_writer.push(Message::Data(2)).unwrap();
        right_writer.push(Message::Data(1)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(reader.read().unwrap(), Message::Data(4));
    }

    /// An input that never had a producer stays open; only a dropped producer closes it.
    #[test]
    fn typed_unfed_left_input_stays_open() {
        let mut biun = Biunion::of(MockBiunion::new());
        Push::<usize>::open(&biun.at::<biunion::Left>());
        let mut right_writer = Writer::new(&biun.at::<biunion::Right>()).unwrap();
        let mut reader = Reader::new(biun).unwrap();

        right_writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(6));
    }

    #[test]
    fn close_propagates_through_push_when_left_input_closed() {
        let mut biun = Biunion::<_, _, _, _, DefaultThread, _>::of(MockBiunion::new());

        let output_edge = Receiver::<usize, Trackable<&'static str>>::new();
        Add::<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>::add(
            &mut biun,
            Box::new(output_edge.sender()),
        )
        .unwrap();

        biun.input.left.close().unwrap();

        let result = biun.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            output_edge.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn close_propagates_through_push_when_right_input_closed() {
        let mut biun = Biunion::<_, _, _, _, DefaultThread, _>::of(MockBiunion::new());

        let output_edge = Receiver::<usize, Trackable<&'static str>>::new();
        Add::<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>::add(
            &mut biun,
            Box::new(output_edge.sender()),
        )
        .unwrap();

        biun.input.right.close().unwrap();

        let result = biun.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            output_edge.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn close_propagates_through_work_chain() {
        use crate::closed;
        use crate::graph::marker::Connection;

        // Create a mock workable that returns Closed error
        struct ClosingWorkable;
        impl Connection for ClosingWorkable {}
        impl Workable for ClosingWorkable {
            type ThreadId = DefaultThread;
            fn work(&mut self) -> Result<(), Error> {
                Err(closed!())
            }
        }

        let mut biun = Biunion::of(MockBiunion::new());

        // Add a workable that returns Closed
        Add::<dyn Workable<ThreadId = DefaultThread>, biunion::Left>::add(
            &mut biun,
            Box::new(ClosingWorkable),
        )
        .unwrap();

        let output_edge = Receiver::<usize, Trackable<&'static str>>::new();
        Add::<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>::add(
            &mut biun,
            Box::new(output_edge.sender()),
        )
        .unwrap();

        let result = biun.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            output_edge.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }
}
