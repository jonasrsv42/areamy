use super::{NodeState, SharedState};
use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Pushable, Receivable};
use crate::node::line::poll::routine::LineRoutine;
use crate::node::routine;
use crate::poll::Pollable;
use crate::poll::waker::Waker;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

impl<In, Out, SignalType, RoutineType, InputType, OutputType>
    SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    /// Poll routine. Builds std Context from sync waker for the routine.
    /// Closed error from routine fatal.
    fn poll_routine(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        match routine::Poll::poll(&mut self.work.target, waker) {
            Ok(poll) => Ok(poll),
            Err(e) if matches!(e.kind, ErrorKind::Closed) => Err(fatal!(
                "routine returned Closed error — routines must handle close, not propagate it"
            )),
            Err(e) => Err(e),
        }
    }
}

/// Work phase: polls routine async work, transitions flush/close states.
pub struct Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    pub(crate) shared:
        Rc<RefCell<SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>>>,
    _thread_id: PhantomData<ThreadIdType>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
    Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    pub(super) fn new(
        shared: Rc<RefCell<SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>>>,
    ) -> Self {
        Self {
            shared,
            _thread_id: PhantomData,
        }
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Connection
    for Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    type ThreadId = ThreadIdType;

    // Work polls the routine. The routine is responsible for waking
    // Output when it produces data (via OutputProducer). Work only
    // wakes Output on state transitions (FlushReady, CloseReady)
    // where Output needs to forward signals downstream.
    fn poll(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Flushing(_) => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                        NodeState::Flushing(sig) => sig,
                        _ => return Err(fatal!("expected Flushing state")),
                    };
                    s.state = NodeState::FlushReady(signal);
                    // Flush complete. Output must drain remaining output
                    // and forward the Flush signal downstream.
                    s.output.waker.wake();
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::Running => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                // No wake — routine is responsible for waking Output
                // when it produces data (via OutputProducer).
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                // Routine got one poll cycle. Output must drain any
                // produced output and forward the marker downstream.
                s.output.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            // FlushReady — Work is done, Output handles it
            NodeState::FlushReady(_) => Ok(core::task::Poll::Pending),
        }
    }
}
