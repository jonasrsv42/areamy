use super::{NodeState, Shared, SharedState};
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Pushable, Receivable};
use crate::message::Message;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::Pollable;
use crate::poll::waker::Waker;
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
    fn push_output(&mut self, msg: Message<Out, SignalType>) -> Result<(), Error> {
        self.output.target.push(msg)
    }

    fn drain_output(&mut self) -> Result<(), Error> {
        while let Some(out) = self.work.target.next()? {
            self.push_output(Message::Data(out))?;
        }
        Ok(())
    }
}

pub struct Output<
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
    Output<
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
    for Output<
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
    for Output<
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
            NodeState::Closed => Err(crate::closed!()),
            NodeState::FlushReady(_) => {
                s.drain_output()?;
                let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                    NodeState::FlushReady(sig) => sig,
                    _ => return Err(fatal!("expected FlushReady state")),
                };
                s.push_output(Message::Flush(signal))?;
                // Flush forwarded, back to Running. Both inputs can resume.
                s.input.left.waker.wake();
                s.input.right.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                s.drain_output()?;
                let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                    NodeState::MarkerReady(sig) => sig,
                    _ => return Err(fatal!("expected MarkerReady state")),
                };
                s.push_output(Message::Marker(signal))?;
                // Marker forwarded, back to Running. Both inputs can resume.
                s.input.left.waker.wake();
                s.input.right.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::CloseReady => {
                s.drain_output()?;
                s.output.target.close()?;
                s.state = NodeState::Closed;
                Err(crate::closed!())
            }
            _ => {
                s.drain_output()?;
                Ok(core::task::Poll::Pending)
            }
        }
    }
}
