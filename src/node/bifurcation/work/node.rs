use crate::closed;
use crate::edge::fanout::Fanout;
use crate::edge::sync::Receiver;
use crate::error::{Error, ErrorKind};
use crate::graph::marker::{Connection, Multiplicity};
use crate::graph::{Add, Closeable, Get, Outputs};
use crate::message::Message;
use crate::node::bifurcation::Sides;
use crate::node::bifurcation::routine::BifurcationRoutine;
use crate::node::bifurcation::work::builder::BifurcationBuilder;
use crate::node::routine::{Flush as _, Send as _};
use crate::node::work::Incoming;
use crate::node::{bifurcation, routine};
use crate::poll;
use crate::signal::Origin;
use crate::thread::ThreadId;
use crate::work::{DynSink, Sink};
use crate::work::{Workable, work_each};
use std::any::type_name;
use std::marker::PhantomData;

// The contract of a `Sync` node forming a bifurcation.
// it has two outputs.
///
/// Every node with these capabilities is a [BifurcationTrait]; there is nothing to implement.
/// Areamy provides a default implementation [Bifurcation].
pub trait BifurcationTrait<'params, In, Left, Right, Signal>:
    // We can work on the line to produce output.
    Workable
    // We can add edges it should push into: a `Push` parent per side.
    + Outputs<
        bifurcation::Left,
        Sink = dyn Sink<DataType = Left, SignalType = Signal> + Send + Sync + 'params,
    >
    + Outputs<
        bifurcation::Right,
        Sink = dyn Sink<DataType = Right, SignalType = Signal> + Send + Sync + 'params,
    >

    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params>

    // We can retrieve its input edge for others to push into, work or poll.
    + Get<dyn Sink<DataType = In, SignalType = Signal> + Send + Sync + 'params>
    + Get<dyn poll::Sink<DataType = In, SignalType = Signal> + Send + Sync + 'params>
where
    Signal: Origin,
{
}

impl<'params, In, Left, Right, Signal, BifurcationType>
    BifurcationTrait<'params, In, Left, Right, Signal> for BifurcationType
where
    Signal: Origin,
    BifurcationType: Workable
        + Outputs<
            bifurcation::Left,
            Sink = dyn Sink<DataType = Left, SignalType = Signal> + Send + Sync + 'params,
        > + Outputs<
            bifurcation::Right,
            Sink = dyn Sink<DataType = Right, SignalType = Signal> + Send + Sync + 'params,
        > + Add<dyn Workable<ThreadId = <BifurcationType as Workable>::ThreadId> + 'params>
        + Get<dyn Sink<DataType = In, SignalType = Signal> + Send + Sync + 'params>
        + Get<dyn poll::Sink<DataType = In, SignalType = Signal> + Send + Sync + 'params>,
{
}

/// The types a [Bifurcation] is built from. Bounds live here once, so impls take a single
/// `T: Types`.
pub trait Types {
    type In: Send + Sync + 'static;
    type Left: Clone + Send + Sync;
    type Right: Clone + Send + Sync;
    type Signal: Origin + Clone + Send + Sync + 'static;
    type ThreadId: ThreadId;
    type Routine: BifurcationRoutine<Self::In, Self::Left, Self::Right>;
}

/// [Types] from its parts, so [Bifurcation::of] infers them from the routine and the wiring.
pub struct Node<In, Left, Right, SignalType, ThreadIdType, RoutineType>(
    PhantomData<fn() -> (In, Left, Right, SignalType, ThreadIdType, RoutineType)>,
);

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType> Types
    for Node<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    type In = In;
    type Left = Left;
    type Right = Right;
    type Signal = SignalType;
    type ThreadId = ThreadIdType;
    type Routine = RoutineType;
}

/// Output edges by side. Each side holds its own refused message; a side suspended on `Full`
/// doesn't stop the other from delivering what the routine already made for it.
pub type Fanouts<'params, T> = Sides<
    Fanout<DynSink<'params, <T as Types>::Left, <T as Types>::Signal>>,
    Fanout<DynSink<'params, <T as Types>::Right, <T as Types>::Signal>>,
>;

/// A Flush or Marker per side, held until that side's routine outputs are out.
pub(super) type Pending<T> = Sides<
    Option<Message<<T as Types>::Left, <T as Types>::Signal>>,
    Option<Message<<T as Types>::Right, <T as Types>::Signal>>,
>;

/// A work node with one input and two outputs.
///
/// To have a child schedule it, connect the side that feeds the child and the scheduling
/// separately, since a bifurcation has two outputs to choose from:
///
/// ```ignore
/// bifurcation.at_mut::<Left>().pushes(&child)?;
/// child.schedules(bifurcation)?;
/// ```
#[must_use = "a node does nothing until owned (`via` / `schedules`) or added to a thread"]
pub struct Bifurcation<'params, T: Types> {
    /// The coroutine of this node.
    pub routine: T::Routine,

    /// Parent workables.
    pub workers: Vec<Box<dyn Workable<ThreadId = T::ThreadId> + 'params>>,

    /// Output edges, grouped by side.
    pub outputs: Fanouts<'params, T>,

    /// Input edge.
    pub input: Receiver<T::In, T::Signal>,

    /// Per side: a Flush or Marker held until that side's routine outputs are out.
    pub(super) pending: Pending<T>,
}

impl<T: Types> Connection for Bifurcation<'_, T> {}

impl<T: Types> Workable for Bifurcation<'_, T> {
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
            match next_input::<T>(workers, input)? {
                Incoming::Message(message) => accept::<T>(routine, pending, message)?,
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

    type ThreadId = T::ThreadId;
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Bifurcation<'params, Node<In, Left, Right, SignalType, ThreadIdType, RoutineType>>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    pub fn of(routine: RoutineType) -> Self {
        Self::builder().build(routine)
    }

    /// Options for a bifurcation, such as [BifurcationBuilder::bounded]; finish with
    /// [BifurcationBuilder::build].
    pub fn builder()
    -> BifurcationBuilder<'params, Node<In, Left, Right, SignalType, ThreadIdType, RoutineType>>
    {
        BifurcationBuilder::new()
    }
}

/// One side's part of a round: finish its suspended push, else push its next routine output,
/// else its pending signal (so a signal goes out behind that side's outputs). True if the side
/// was busy, false if idle. The side is passed by value so the routine's `Next` impl for it is
/// inferred.
fn side_round<Out, Side, SignalType, RoutineType>(
    routine: &mut RoutineType,
    outputs: &mut Fanout<DynSink<'_, Out, SignalType>>,
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
fn next_input<T: Types>(
    workers: &mut Vec<Box<dyn Workable<ThreadId = T::ThreadId> + '_>>,
    input: &Receiver<T::In, T::Signal>,
) -> Result<Incoming<Message<T::In, T::Signal>>, Error> {
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
fn accept<T: Types>(
    routine: &mut T::Routine,
    pending: &mut Pending<T>,
    message: Message<T::In, T::Signal>,
) -> Result<(), Error> {
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

/// Get a sink for this node's input edge.
impl<'params, ConnectionType, T: Types> Get<ConnectionType> for Bifurcation<'params, T>
where
    ConnectionType: Connection + ?Sized,
    Receiver<T::In, T::Signal>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input)
    }
}

impl<'params, T: Types>
    Add<
        dyn Sink<DataType = T::Left, SignalType = T::Signal> + Send + Sync + 'params,
        bifurcation::Left,
    > for Bifurcation<'params, T>
{
    fn add(
        &mut self,
        closeable: Box<
            dyn Sink<DataType = T::Left, SignalType = T::Signal> + Send + Sync + 'params,
        >,
    ) -> Result<(), Error> {
        self.outputs.left.add(closeable);
        Ok(())
    }
}

impl<'params, T: Types>
    Add<
        dyn Sink<DataType = T::Right, SignalType = T::Signal> + Send + Sync + 'params,
        bifurcation::Right,
    > for Bifurcation<'params, T>
{
    fn add(
        &mut self,
        closeable: Box<
            dyn Sink<DataType = T::Right, SignalType = T::Signal> + Send + Sync + 'params,
        >,
    ) -> Result<(), Error> {
        self.outputs.right.add(closeable);
        Ok(())
    }
}

impl<'params, T: Types> Outputs<bifurcation::Left> for Bifurcation<'params, T> {
    type Sink = dyn Sink<DataType = T::Left, SignalType = T::Signal> + Send + Sync + 'params;
}

impl<'params, T: Types> Outputs<bifurcation::Right> for Bifurcation<'params, T> {
    type Sink = dyn Sink<DataType = T::Right, SignalType = T::Signal> + Send + Sync + 'params;
}

impl<'params, T: Types> Add<dyn Workable<ThreadId = T::ThreadId> + 'params>
    for Bifurcation<'params, T>
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = T::ThreadId> + 'params>,
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
    use crate::work::Writer;
    use crate::{At, DefaultThread, Tees, Trackable, Writes};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    type TestSignal = Trackable<&'static str>;
    type TestBifurcation<'params, R> =
        Bifurcation<'params, Node<usize, usize, usize, TestSignal, DefaultThread, R>>;
    type Hold = TestBifurcation<'static, HoldBifurcation>;

    /// [Bifurcation] meets the [BifurcationTrait] contract.
    #[test]
    fn bifurcation_meets_bifurcation_trait() {
        fn contract<'params, B: BifurcationTrait<'params, usize, usize, usize, TestSignal>>(_: &B) {
        }
        let bifurcation: TestBifurcation<MockBifurcation> = Bifurcation::of(MockBifurcation::new());
        contract(&bifurcation);
    }
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
        Add::add(&mut bifurcation.at_mut::<bifurcation::Left>(), left_edge).unwrap();
        Add::add(&mut bifurcation.at_mut::<bifurcation::Right>(), right_edge).unwrap();
        let writer = bifurcation.writer().unwrap();
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

        let mut writer = bifur.writer().unwrap();

        let mut left_reader = bifur.at_mut::<bifurcation::Left>().tee().unwrap();
        let mut right_reader = bifur.at_mut::<bifurcation::Right>().tee().unwrap();

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
        let mut bifur =
            Bifurcation::<Node<_, _, _, _, DefaultThread, _>>::of(MockBifurcation::new());

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
