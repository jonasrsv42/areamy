use super::{NodeState, SharedState};
use crate::error::{Error, ErrorKind};
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Pushable, Receivable};
use crate::message::Message;
use crate::node::line::poll::routine::LineRoutine;
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
    /// Drain input edge. Forwards data via [crate::node::routine::Send].
    /// May transition state to Flushing, Closing, or MarkerReady.
    fn drain_input(&mut self) -> Result<(), Error> {
        loop {
            match self.input.target.try_recv() {
                Ok(Some(Message::Data(data))) => {
                    self.work.target.send(data)?;
                }
                Ok(Some(Message::Flush(signal))) => {
                    self.work.target.flush()?;
                    self.state = NodeState::Flushing(signal);
                    // Work must poll routine until Ready to complete the flush.
                    self.work.waker.wake();
                    break;
                }
                Ok(Some(Message::Marker(signal))) => {
                    self.state = NodeState::MarkerReady(signal);
                    // Work gets one poll cycle to let routine emit pending output
                    // before Output forwards the marker.
                    self.work.waker.wake();
                    break;
                }
                Ok(None) => break,
                Err(e) => {
                    if matches!(e.kind, ErrorKind::Closed) {
                        // Fast close: skip flush, skip routine polling.
                        // Output drains buffered next() and closes the
                        // downstream edge; routine is dropped with the node.
                        // Wake Work so it observes CloseReady and reports
                        // closed back to the poll loop; wake Output to
                        // drive the drain + close.
                        self.state = NodeState::CloseReady;
                        self.work.waker.wake();
                        self.output.waker.wake();
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

/// Input phase: drains edge into routine, wakes Output.
pub struct Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
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
    Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
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
    for Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_input()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            // Flushing/FlushReady/MarkerReady — Input waits for Output to transition back to Running
            NodeState::Flushing(_) | NodeState::FlushReady(_) | NodeState::MarkerReady(_) => {
                Ok(core::task::Poll::Pending)
            }
        }
    }
}
