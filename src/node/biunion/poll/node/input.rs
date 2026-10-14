use super::{NodeState, Shared, SharedState};
use crate::error::{Error, ErrorKind};
use crate::graph::marker::{Connection, Multiplicity};
use crate::graph::{Closeable, Pushable, Receivable};
use crate::message::Message;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::node::{biunion, routine};
use crate::poll::waker::{ThreadLocalWaker, Waker};
use crate::poll::{Phase, Pollable};
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

impl<Left, Right, Out, SignalType, RoutineType, LeftInputType, RightInputType, OutputType>
    SharedState<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    fn drain_left(&mut self) -> Result<(), Error> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = self;
        drain(
            biunion::Left,
            &mut input.left.target,
            &input.right.waker,
            work,
            &output.waker,
            state,
        )
    }

    fn drain_right(&mut self) -> Result<(), Error> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = self;
        drain(
            biunion::Right,
            &mut input.right.target,
            &input.left.waker,
            work,
            &output.waker,
            state,
        )
    }
}

/// Drain one side's edge into the routine as `Side`. May transition state to Flushing,
/// MarkerReady or CloseReady. `other` is the opposite input, woken on close.
fn drain<Data, SignalType, Side, Edge, RoutineType>(
    _side: Side,
    edge: &mut Edge,
    other: &ThreadLocalWaker,
    work: &mut Phase<RoutineType>,
    output: &ThreadLocalWaker,
    state: &mut NodeState<SignalType>,
) -> Result<(), Error>
where
    SignalType: Origin,
    Side: Multiplicity,
    Edge: Receivable<DataType = Data, SignalType = SignalType>,
    RoutineType: routine::Send<Data, Side> + routine::Flush,
{
    loop {
        match edge.try_recv() {
            Ok(Some(Message::Data(data))) => {
                work.target.send(data)?;
            }
            Ok(Some(Message::Flush(signal))) => {
                work.target.flush()?;
                *state = NodeState::Flushing(signal);
                // Work must poll routine until Ready to complete the flush.
                work.waker.wake();
                break;
            }
            Ok(Some(Message::Marker(signal))) => {
                *state = NodeState::MarkerReady(signal);
                // Work gets one poll cycle before Output forwards the marker.
                work.waker.wake();
                break;
            }
            Ok(None) => break,
            Err(e) => {
                if matches!(e.kind, ErrorKind::Closed) {
                    // Fast close: skip flush, skip routine polling.
                    // Output drains buffered next() and closes the
                    // downstream edge; routine drops with the node.
                    *state = NodeState::CloseReady;
                    work.waker.wake();
                    output.wake();
                    // Wake the other input so it sees CloseReady and
                    // returns Closed.
                    other.wake();
                    break;
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

pub struct LeftInput<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    shared: Shared<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
    _thread_id: PhantomData<ThreadIdType>,
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
>
    LeftInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    pub(super) fn new(
        shared: Shared<
            Left,
            Right,
            Out,
            SignalType,
            RoutineType,
            LeftInputType,
            RightInputType,
            OutputType,
        >,
    ) -> Self {
        Self {
            shared,
            _thread_id: PhantomData,
        }
    }
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Connection
    for LeftInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Pollable
    for LeftInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_left()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            _ => Ok(core::task::Poll::Pending),
        }
    }
}

pub struct RightInput<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    shared: Shared<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
    _thread_id: PhantomData<ThreadIdType>,
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
>
    RightInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    pub(super) fn new(
        shared: Shared<
            Left,
            Right,
            Out,
            SignalType,
            RoutineType,
            LeftInputType,
            RightInputType,
            OutputType,
        >,
    ) -> Self {
        Self {
            shared,
            _thread_id: PhantomData,
        }
    }
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Connection
    for RightInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Pollable
    for RightInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_right()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            _ => Ok(core::task::Poll::Pending),
        }
    }
}
