use crate::closed;
use crate::edge::fanout::Fanout;
use crate::edge::sync::Receiver;
use crate::error::{Error, ErrorKind};
use crate::graph::marker::{Connection, Multiplicity};
use crate::graph::{Add, Get, Outputs, Pushable};
use crate::message::Message;
use crate::node::bifurcation::routine::BifurcationRoutine;
use crate::node::bifurcation::work::builder::BifurcationBuilder;
use crate::node::work::Incoming;
use crate::node::{bifurcation, routine};
use crate::signal::Origin;
use crate::thread::ThreadId;
use crate::work::Sink;
use crate::work::{Workable, work_each};
use std::any::type_name;
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a bifurcation.
// it has two outputs.
pub trait BifurcationTrait<'params>:
    // We can work on the line to produce output.
    Workable
    // We can add edges it should push into.
    + Add<dyn Sink<DataType = Self::Left, SignalType = Self::Signal> + Send + Sync + 'params, bifurcation::Left>
    + Add<dyn Sink<DataType = Self::Right, SignalType = Self::Signal> + Send + Sync + 'params, bifurcation::Right>

    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params>

    // We can retrieve pushable edges
    + Get<dyn Pushable<DataType = Self::In, SignalType = Self::Signal> + 'params>

    // We can retrieve a Closeable for closing the input edge.
    + Get<dyn Sink<DataType = Self::In, SignalType = Self::Signal> + Send + Sync + 'params>
{
    // The input data entering it.
    type In: Send + Sync + 'static;
    // The output data going out of the bifurcation.
    type Left: Clone + Send + Sync;
    type Right: Clone + Send + Sync;
    // The signal type used in the graph.
    type Signal: Origin + Clone + 'static;
    // The coroutine associated with this node.
    type BifurcationRoutine: BifurcationRoutine<Self::In, Self::Left, Self::Right>;

}

impl<'params, BifurcationType: BifurcationTrait<'params>> BifurcationTrait<'params>
    for Arc<Mutex<BifurcationType>>
{
    type In = BifurcationType::In;
    type Left = BifurcationType::Left;
    type Right = BifurcationType::Right;
    type Signal = BifurcationType::Signal;
    type BifurcationRoutine = BifurcationType::BifurcationRoutine;
}

/// One value per bifurcation side.
pub struct Sides<Left, Right> {
    pub left: Left,
    pub right: Right,
}

/// Output edges by side. Each side holds its own refused message; a side suspended on `Full`
/// doesn't stop the other from delivering what the routine already made for it.
pub type Fanouts<'params, Left, Right, SignalType> =
    Sides<Fanout<'params, Left, SignalType>, Fanout<'params, Right, SignalType>>;

/// A Flush or Marker per side, held until that side's routine outputs are out.
pub(super) type Pending<Left, Right, SignalType> =
    Sides<Option<Message<Left, SignalType>>, Option<Message<Right, SignalType>>>;

/// A work node with one input and two outputs.
///
/// To have a child schedule it, connect the side that feeds the child and the scheduling
/// separately, since a bifurcation has two outputs to choose from:
///
/// ```ignore
/// Push::connect(&mut bifurcation.at::<Left>(), &child)?;
/// work::Schedule::connect(bifurcation, &mut child)?;
/// ```
pub struct Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    /// The coroutine of this node.
    pub routine: RoutineType,

    /// Parent workables.
    pub workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,

    /// Output edges, grouped by side.
    pub outputs: Fanouts<'params, Left, Right, SignalType>,

    /// Input edge.
    pub input: Receiver<In, SignalType>,

    /// Per side: a Flush or Marker held until that side's routine outputs are out.
    pub(super) pending: Pending<Left, Right, SignalType>,
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType> Connection
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType> Workable
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    /// At most one message per side per round: each side finishes its suspended push, else
    /// pushes its next routine output or pending signal. Input is taken only once both sides
    /// are idle, so a suspended side stops input and the routine's buffer holds the rest.
    fn work(&mut self) -> Result<(), Error> {
        let Bifurcation {
            routine,
            workers,
            outputs,
            input,
            pending,
        } = self;

        loop {
            let left = side_round(
                routine,
                &mut outputs.left,
                &mut pending.left,
                bifurcation::Left {},
            )?;
            let right = side_round(
                routine,
                &mut outputs.right,
                &mut pending.right,
                bifurcation::Right {},
            )?;
            if left || right {
                return Ok(());
            }

            // Both sides idle: nothing held, nothing left in the routine, no signal owed.
            match next_input(workers, input)? {
                Incoming::Message(message) => accept(routine, pending, message)?,
                Incoming::Closed => {
                    // Close the outputs too, so children see it. Other errors, the routine's
                    // `Closed` included, leave them to close on drop.
                    let _ = outputs.left.close();
                    let _ = outputs.right.close();
                    return Err(closed!());
                }
            }
        }
    }

    type ThreadId = ThreadIdType;
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    pub fn of(routine: RoutineType) -> Self {
        Self::builder().build(routine)
    }

    /// Options for a bifurcation, such as [BifurcationBuilder::bounded]; finish with
    /// [BifurcationBuilder::build].
    pub fn builder()
    -> BifurcationBuilder<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType> {
        BifurcationBuilder::new()
    }
}

/// One side's part of a round: finish its suspended push, else push its next routine output,
/// else its pending signal (so a signal goes out behind that side's outputs). True if the side
/// was busy, false if idle. The side is passed by value so the routine's `Next` impl for it is
/// inferred.
fn side_round<Out, Side, SignalType, RoutineType>(
    routine: &mut RoutineType,
    outputs: &mut Fanout<'_, Out, SignalType>,
    pending: &mut Option<Message<Out, SignalType>>,
    _side: Side,
) -> Result<bool, Error>
where
    Out: Clone,
    Side: Multiplicity,
    SignalType: Origin + Clone,
    RoutineType: routine::Next<Out, Side>,
{
    let Some(ready) = outputs.ready() else {
        // Busy even when the resume completes: taking input now could block after a push.
        let _ = outputs.resume(type_name::<RoutineType>())?;
        return Ok(true);
    };
    let Some(message) = routine
        .next()?
        .map(Message::Data)
        .or_else(|| pending.take())
    else {
        return Ok(false);
    };
    // A refused message stays in the fan-out; the next round resumes it.
    let _ = ready.push(message)?;
    Ok(true)
}

/// The next input message, working parents while the input is empty. Parents are scheduled
/// only once input polled empty, so a parent refused by this input always finds progress
/// (`Full`) when re-entered.
fn next_input<'params, In, SignalType, ThreadIdType>(
    workers: &mut Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,
    input: &Receiver<In, SignalType>,
) -> Result<Incoming<Message<In, SignalType>>, Error>
where
    In: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    loop {
        match input.poll() {
            Ok(Some(message)) => return Ok(Incoming::Message(message)),
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

/// Take one input message: data goes to the routine, a signal is owed to both sides.
fn accept<In, Left, Right, SignalType, RoutineType>(
    routine: &mut RoutineType,
    pending: &mut Pending<Left, Right, SignalType>,
    message: Message<In, SignalType>,
) -> Result<(), Error>
where
    Left: Clone,
    Right: Clone,
    SignalType: Origin + Clone,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    match message {
        Message::Data(data) => routine.send(data)?,
        Message::Flush(origin) => {
            routine.flush()?;
            *pending = Sides {
                left: Some(Message::Flush(origin.clone())),
                right: Some(Message::Flush(origin)),
            };
        }
        Message::Marker(origin) => {
            *pending = Sides {
                left: Some(Message::Marker(origin.clone())),
                right: Some(Message::Marker(origin)),
            };
        }
    }
    Ok(())
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType> BifurcationTrait<'params>
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    type In = In;
    type Left = Left;
    type Right = Right;
    type Signal = SignalType;
    type BifurcationRoutine = RoutineType;
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = In, SignalType = SignalType> + 'params>
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = In, SignalType = SignalType> + 'params>, Error> {
        Get::get(&self.input)
    }
}

/// Get a [Sink] for this node's input edge.
impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Get<dyn Sink<DataType = In, SignalType = SignalType> + Send + Sync + 'params>
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Sink<DataType = In, SignalType = SignalType> + Send + Sync + 'params>, Error>
    {
        Get::get(&self.input)
    }
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<
        dyn Sink<DataType = Left, SignalType = SignalType> + Send + Sync + 'params,
        bifurcation::Left,
    > for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    fn add(
        &mut self,
        closeable: Box<dyn Sink<DataType = Left, SignalType = SignalType> + Send + Sync + 'params>,
    ) -> Result<(), Error> {
        self.outputs.left.add(closeable);
        Ok(())
    }
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<
        dyn Sink<DataType = Right, SignalType = SignalType> + Send + Sync + 'params,
        bifurcation::Right,
    > for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    fn add(
        &mut self,
        closeable: Box<dyn Sink<DataType = Right, SignalType = SignalType> + Send + Sync + 'params>,
    ) -> Result<(), Error> {
        self.outputs.right.add(closeable);
        Ok(())
    }
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType> Outputs<bifurcation::Left>
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    type Sink = dyn Sink<DataType = Left, SignalType = SignalType> + Send + Sync + 'params;
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType> Outputs<bifurcation::Right>
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    type Sink = dyn Sink<DataType = Right, SignalType = SignalType> + Send + Sync + 'params;
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType> + 'params>
    for Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    ) -> Result<(), Error> {
        self.workers.push(workable);
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::Pushable;
    use crate::closed;
    use crate::edge::sync::tests::{join_within, wait_until};
    use crate::graph::Closeable;
    use crate::node::bifurcation::routine::tests::{HoldBifurcation, MockBifurcation};
    use crate::work::{Writer, tee};
    use crate::{At, DefaultThread, Trackable};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    type TestSignal = Trackable<&'static str>;
    type Hold =
        Bifurcation<'static, usize, usize, usize, TestSignal, DefaultThread, HoldBifurcation>;
    type Edge = Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync>;

    fn receiver(bound: Option<NonZeroUsize>) -> Receiver<usize, TestSignal> {
        match bound {
            Some(bound) => Receiver::bounded(bound),
            None => Receiver::new(),
        }
    }

    /// A hold bifurcation with one raw output edge per side (`None` is unbounded) and a writer
    /// into its input.
    fn wired(
        wait: usize,
        left: Option<NonZeroUsize>,
        right: Option<NonZeroUsize>,
    ) -> (
        Hold,
        Writer<'static, usize>,
        Receiver<usize, TestSignal>,
        Receiver<usize, TestSignal>,
    ) {
        let mut bifurcation: Hold = Bifurcation::of(HoldBifurcation::new(wait));
        let (left, right) = (receiver(left), receiver(right));
        let left_edge: Edge = Box::new(left.sender());
        let right_edge: Edge = Box::new(right.sender());
        Add::add(&mut bifurcation.at::<bifurcation::Left>(), left_edge).unwrap();
        Add::add(&mut bifurcation.at::<bifurcation::Right>(), right_edge).unwrap();
        let writer = Writer::new(&bifurcation).unwrap();
        (bifurcation, writer, left, right)
    }

    fn drain(receiver: &Receiver<usize, TestSignal>) -> Vec<Message<usize, TestSignal>> {
        std::iter::from_fn(|| receiver.poll().unwrap()).collect()
    }

    #[test]
    fn marker_goes_out_behind_each_sides_outputs() {
        let (mut bifurcation, mut writer, left, right) = wired(2, None, None);
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Marker("m".into())).unwrap();
        // Closed, so a round that finds the input empty fails instead of waiting.
        writer.close().unwrap();

        for _ in 0..3 {
            bifurcation.work().unwrap();
        }
        let expected = vec![
            Message::Data(1),
            Message::Data(2),
            Message::Marker("m".into()),
        ];
        assert_eq!(drain(&left), expected);
        assert_eq!(drain(&right), expected);
    }

    /// A parent that only counts how often it is worked.
    struct Counted(Arc<AtomicUsize>);

    impl Connection for Counted {}

    impl Workable for Counted {
        type ThreadId = DefaultThread;
        fn work(&mut self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn own_input_goes_out_before_parents_are_worked() {
        let (mut bifurcation, mut writer, left, _right) = wired(1, None, None);
        let calls = Arc::new(AtomicUsize::new(0));
        Add::<dyn Workable<ThreadId = DefaultThread>>::add(
            &mut bifurcation,
            Box::new(Counted(calls.clone())),
        )
        .unwrap();
        writer.push(Message::Data(1)).unwrap();

        bifurcation.work().unwrap();
        assert_eq!(left.poll().unwrap(), Some(Message::Data(1)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn flush_goes_out_after_everything_it_released_on_each_side() {
        let (mut bifurcation, mut writer, left, right) = wired(10, None, None);
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Flush("f".into())).unwrap();
        // Closed, so a round that finds the input empty fails instead of waiting.
        writer.close().unwrap();

        for _ in 0..3 {
            bifurcation.work().unwrap();
        }
        // The flush releases both held messages; on each side the Flush comes after them.
        let expected = vec![
            Message::Data(1),
            Message::Data(2),
            Message::Flush("f".into()),
        ];
        assert_eq!(drain(&left), expected);
        assert_eq!(drain(&right), expected);
    }

    #[test]
    fn suspended_side_stops_input_while_the_other_drains() {
        // Holds 3 inputs, then releases them to both sides; left holds at most one.
        let (mut bifurcation, mut writer, left, right) = wired(3, NonZeroUsize::new(1), None);
        let refill = left.sender();
        for value in 1..=4 {
            writer.push(Message::Data(value)).unwrap();
        }

        bifurcation.work().unwrap(); // left 1, right 1
        bifurcation.work().unwrap(); // left 2 refused (`Full`), right 2
        assert_eq!(bifurcation.outputs.left.suspended(), Some(0));

        // Left drains but is refilled: `Full` again, so left stays suspended without blocking.
        assert_eq!(left.poll().unwrap(), Some(Message::Data(1)));
        refill.push_back(Message::Data(99)).unwrap();
        bifurcation.work().unwrap();
        assert_eq!(bifurcation.outputs.left.suspended(), Some(0));

        // Right still got what the routine had already made for it.
        assert_eq!(
            drain(&right),
            vec![Message::Data(1), Message::Data(2), Message::Data(3)]
        );
        // But no new input was taken while left is stuck.
        assert_eq!(bifurcation.input.len().unwrap(), 1);
    }

    #[test]
    fn each_side_sends_the_flush_behind_its_own_outputs() {
        let (mut bifurcation, mut writer, left, right) = wired(10, NonZeroUsize::new(1), None);
        let refill = left.sender();
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Flush("f".into())).unwrap();
        // Closed, so a round that finds the input empty fails instead of waiting.
        writer.close().unwrap();

        bifurcation.work().unwrap(); // left 1, right 1
        bifurcation.work().unwrap(); // left 2 refused, right 2
        assert_eq!(left.poll().unwrap(), Some(Message::Data(1)));
        refill.push_back(Message::Data(99)).unwrap();
        bifurcation.work().unwrap(); // left 2 refused again, right Flush

        // Right is done while left still owes 2 and the Flush.
        assert_eq!(
            drain(&right),
            vec![
                Message::Data(1),
                Message::Data(2),
                Message::Flush("f".into())
            ]
        );
        assert_eq!(left.poll().unwrap(), Some(Message::Data(99)));
        bifurcation.work().unwrap(); // left 2
        assert_eq!(left.poll().unwrap(), Some(Message::Data(2)));
        bifurcation.work().unwrap(); // left Flush
        assert_eq!(left.poll().unwrap(), Some(Message::Flush("f".into())));
    }

    #[test]
    fn full_side_path_yields_once_then_blocks_until_a_pop() {
        let (mut bifurcation, mut writer, left, _right) = wired(1, NonZeroUsize::new(1), None);
        for value in 0..3 {
            writer.push(Message::Data(value)).unwrap();
        }

        bifurcation.work().unwrap();
        // Refused with `Full`: the bifurcation yields instead of blocking.
        bifurcation.work().unwrap();
        assert_eq!(bifurcation.outputs.left.suspended(), Some(0));

        // Nothing popped since: `Stuck`, so this round blocks until the consumer pops.
        let working = thread::spawn(move || bifurcation.work().map(|()| bifurcation));
        wait_until(|| left.producers_waiting() == 1);
        assert_eq!(left.poll().unwrap(), Some(Message::Data(0)));
        let bifurcation = join_within(working).unwrap();
        assert_eq!(bifurcation.outputs.left.suspended(), None);
        assert_eq!(left.poll().unwrap(), Some(Message::Data(1)));
    }

    #[test]
    fn run_bifurcation() {
        let mut bifur = Bifurcation::of(MockBifurcation::new());

        let mut writer = Writer::new(&bifur).unwrap();

        let mut left_reader = tee::Reader::new(&mut bifur.at::<bifurcation::Left>()).unwrap();
        let mut right_reader = tee::Reader::new(&mut bifur.at::<bifurcation::Right>()).unwrap();

        let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = Box::new(bifur);

        // Add one flush
        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();

        workable.work().unwrap();
        workable.work().unwrap();

        assert_eq!(left_reader.read().unwrap(), Message::Data(2));
        assert_eq!(left_reader.read().unwrap(), Message::Data(5));

        writer.push(Message::Flush("hi".into())).unwrap();
        workable.work().unwrap();

        assert_eq!(right_reader.read().unwrap(), Message::Data(3));
        assert_eq!(right_reader.read().unwrap(), Message::Data(7));

        // Now comes the flush
        assert!(matches!(right_reader.read().unwrap(), Message::Flush(_)));
        assert!(matches!(left_reader.read().unwrap(), Message::Flush(_)));

        writer.push(Message::Data(2)).unwrap();
        workable.work().unwrap();

        assert_eq!(left_reader.read().unwrap(), Message::Data(4));
        assert_eq!(right_reader.read().unwrap(), Message::Data(6));
    }

    #[test]
    fn close_propagates_through_push_when_input_closed() {
        let mut bifur = Bifurcation::<_, _, _, _, DefaultThread, _>::of(MockBifurcation::new());

        let left_output = Receiver::<usize, Trackable<&'static str>>::new();
        let right_output = Receiver::<usize, Trackable<&'static str>>::new();

        Add::<
            dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
            bifurcation::Left,
        >::add(&mut bifur, Box::new(left_output.sender()))
        .unwrap();
        Add::<
            dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
            bifurcation::Right,
        >::add(&mut bifur, Box::new(right_output.sender()))
        .unwrap();

        bifur.input.close().unwrap();

        let result = bifur.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            left_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
        assert!(matches!(
            right_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn close_propagates_through_work_chain() {
        // Create a mock workable that returns Closed error
        struct ClosingWorkable;
        impl Connection for ClosingWorkable {}
        impl Workable for ClosingWorkable {
            type ThreadId = DefaultThread;
            fn work(&mut self) -> Result<(), Error> {
                Err(closed!())
            }
        }

        let mut bifur = Bifurcation::of(MockBifurcation::new());

        // Add a workable that returns Closed
        Add::<dyn Workable<ThreadId = DefaultThread>>::add(&mut bifur, Box::new(ClosingWorkable))
            .unwrap();

        let left_output = Receiver::<usize, Trackable<&'static str>>::new();
        let right_output = Receiver::<usize, Trackable<&'static str>>::new();

        Add::<
            dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
            bifurcation::Left,
        >::add(&mut bifur, Box::new(left_output.sender()))
        .unwrap();
        Add::<
            dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
            bifurcation::Right,
        >::add(&mut bifur, Box::new(right_output.sender()))
        .unwrap();

        let result = bifur.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            left_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
        assert!(matches!(
            right_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }
}
