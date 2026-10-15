use super::NodeState;
use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::node::routine;
use crate::poll::waker::{LocalWaker, Waker};
use std::mem;

/// Work's borrow of a node's state: polls the routine's async work, completes a flush and wakes
/// Output on the transitions it must forward. The routine wakes Output itself when it produces
/// data (via OutputProducer).
pub(crate) struct Drive<'a, RoutineType, SignalType> {
    pub(crate) routine: &'a mut RoutineType,
    /// Output's waker.
    pub(crate) output: &'a LocalWaker,
    pub(crate) state: &'a mut NodeState<SignalType>,
}

impl<RoutineType: routine::Poll, SignalType> Drive<'_, RoutineType, SignalType> {
    pub(crate) fn poll(mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        match &self.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Flushing(_) => {
                if self.poll_routine(waker)?.is_ready() {
                    let NodeState::Flushing(signal) = mem::replace(self.state, NodeState::Running)
                    else {
                        return Err(fatal!("expected Flushing state"));
                    };
                    *self.state = NodeState::FlushReady(signal);
                    // Flush complete. Output must drain remaining output
                    // and forward the Flush signal downstream.
                    self.output.wake();
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::Running => {
                if self.poll_routine(waker)?.is_ready() {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                // No wake — routine is responsible for waking Output
                // when it produces data (via OutputProducer).
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                if self.poll_routine(waker)?.is_ready() {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                // Routine got one poll cycle. Output must drain any
                // produced output and forward the marker downstream.
                self.output.wake();
                Ok(core::task::Poll::Pending)
            }
            // FlushReady — Work is done, Output handles it
            NodeState::FlushReady(_) => Ok(core::task::Poll::Pending),
        }
    }

    /// Poll the routine. `Closed` from a routine is fatal: routines handle close, never propagate
    /// it.
    fn poll_routine(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        match routine::Poll::poll(self.routine, waker) {
            Ok(poll) => Ok(poll),
            Err(e) if matches!(e.kind, ErrorKind::Closed) => Err(fatal!(
                "routine returned Closed error — routines must handle close, not propagate it"
            )),
            Err(e) => Err(e),
        }
    }
}
