use super::{NodeState, Shared, SharedState, Types};
use crate::error::Error;
use crate::graph::marker::{Connection, Unary};
use crate::node::poll::Feed;
use crate::poll::Pollable;
use crate::poll::waker::Waker;

impl<T: Types> SharedState<T> {
    /// Feed the routine from the input. On close, wake Output to drain and close downstream.
    fn drain(&mut self, held: &mut Option<T::In>) -> Result<(), Error> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = self;
        Feed {
            inlet: &mut input.target,
            waker: &input.waker,
            work,
            state,
        }
        .drain::<Unary>(held)?;
        if matches!(state, NodeState::CloseReady) {
            output.waker.wake();
        }
        Ok(())
    }
}

/// Input phase: drains edge into routine, wakes Output.
pub struct Input<T: Types> {
    pub(crate) shared: Shared<T>,
    /// A message the routine refused; retried before anything else is taken. Only ever set in
    /// Running: while holding, Input reads no signal that could change state.
    held: Option<T::In>,
}

impl<T: Types> Input<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared, held: None }
    }
}

impl<T: Types> Connection for Input<T> {}

impl<T: Types> Pollable for Input<T> {
    type ThreadId = T::ThreadId;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain(&mut self.held)?;
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
