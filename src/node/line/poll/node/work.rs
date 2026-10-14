use super::{NodeState, Shared, SharedState, Types};
use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::graph::marker::Connection;
use crate::node::routine;
use crate::poll::Pollable;
use crate::poll::waker::Waker;

impl<T: Types> SharedState<T> {
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
pub struct Work<T: Types> {
    pub(crate) shared: Shared<T>,
}

impl<T: Types> Work<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }
}

impl<T: Types> Connection for Work<T> {}

impl<T: Types> Pollable for Work<T> {
    type ThreadId = T::ThreadId;

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
