use super::{NodeState, Shared, SharedState, Types};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closed, Receivable};
use crate::node::biunion;
use crate::node::poll::Feed;
use crate::poll::Context;
use crate::poll::Pollable;

/// A biunion input side: the data and inlet it carries.
pub trait InputSide<T: Types>: biunion::Side {
    type Data;
    type Inlet: Receivable<DataType = Self::Data, SignalType = T::Signal> + Closed;
}

/// How a side drains into the routine.
pub(super) trait Feeding<T: Types>: InputSide<T> {
    /// Feed the routine as this side. On close, wake Output to drain and close downstream, and the
    /// other side so it returns Closed.
    fn drain(state: &mut SharedState<T>, held: &mut Option<Self::Data>) -> Result<(), Error>;
}

impl<T: Types> InputSide<T> for biunion::Left {
    type Data = T::Left;
    type Inlet = T::LeftInlet;
}

impl<T: Types> InputSide<T> for biunion::Right {
    type Data = T::Right;
    type Inlet = T::RightInlet;
}

impl<T: Types> Feeding<T> for biunion::Left {
    fn drain(state: &mut SharedState<T>, held: &mut Option<T::Left>) -> Result<(), Error> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = state;
        Feed {
            inlet: &mut input.left.target,
            waker: &input.left.waker,
            work,
            state,
        }
        .drain::<biunion::Left>(held)?;
        if matches!(state, NodeState::CloseReady) {
            output.waker.wake();
            input.right.waker.wake();
        }
        Ok(())
    }
}

impl<T: Types> Feeding<T> for biunion::Right {
    fn drain(state: &mut SharedState<T>, held: &mut Option<T::Right>) -> Result<(), Error> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = state;
        Feed {
            inlet: &mut input.right.target,
            waker: &input.right.waker,
            work,
            state,
        }
        .drain::<biunion::Right>(held)?;
        if matches!(state, NodeState::CloseReady) {
            output.waker.wake();
            input.left.waker.wake();
        }
        Ok(())
    }
}

/// Input phase for side `S`: drains its edge into the routine.
pub struct Input<T: Types, S: InputSide<T>> {
    shared: Shared<T>,
    /// A message the routine refused; retried before anything else is taken. Set only in
    /// Running, but the other side may change state meanwhile: Output's return to Running wakes
    /// both sides, so the retry comes then; CloseReady drops it.
    held: Option<S::Data>,
}

impl<T: Types, S: InputSide<T>> Input<T, S> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared, held: None }
    }
}

impl<T: Types, S: InputSide<T>> Connection for Input<T, S> {}

impl<T: Types, S: Feeding<T>> Pollable for Input<T, S> {
    type ThreadId = T::ThreadId;

    fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                S::drain(&mut s, &mut self.held)?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            // Input waits for Output to transition back to Running
            NodeState::Flushing(_)
            | NodeState::FlushReady(_)
            | NodeState::MarkerReady(_)
            | NodeState::Forwarding => Ok(core::task::Poll::Pending),
        }
    }
}
