use super::{NodeState, Shared, SharedState, Types};
use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Closed, Receivable};
use crate::message::Message;
use crate::node::routine::{Accept, Flush as _};
use crate::node::{biunion, routine};
use crate::poll::waker::{ThreadLocalWaker, Waker};
use crate::poll::{BUDGET, Phase, Pollable};
use std::ops::ControlFlow::{self, Break, Continue};

/// A biunion input side: the data and inlet it carries.
pub trait InputSide<T: Types>: biunion::Side {
    type Data;
    type Inlet: Receivable<DataType = Self::Data, SignalType = T::Signal> + Closed;
}

/// How a side reaches its half of the shared state and its routine input.
pub(super) trait Feeding<T: Types>: InputSide<T> + Sized {
    /// This side's borrow of the state: its inlet and waker, the other side's waker, the rest
    /// shared.
    fn feed(state: &mut SharedState<T>) -> Feed<'_, T, Self>;

    /// [routine::TrySend] to the routine as this side.
    fn try_send(routine: &mut T::Routine, data: Self::Data) -> Result<Accept<Self::Data>, Error>;
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
    fn feed(state: &mut SharedState<T>) -> Feed<'_, T, Self> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = state;
        Feed {
            inlet: &mut input.left.target,
            waker: &input.left.waker,
            other: &input.right.waker,
            work,
            output: &output.waker,
            state,
        }
    }

    fn try_send(routine: &mut T::Routine, data: T::Left) -> Result<Accept<T::Left>, Error> {
        routine::TrySend::<T::Left, biunion::Left>::try_send(routine, data)
    }
}

impl<T: Types> Feeding<T> for biunion::Right {
    fn feed(state: &mut SharedState<T>) -> Feed<'_, T, Self> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = state;
        Feed {
            inlet: &mut input.right.target,
            waker: &input.right.waker,
            other: &input.left.waker,
            work,
            output: &output.waker,
            state,
        }
    }

    fn try_send(routine: &mut T::Routine, data: T::Right) -> Result<Accept<T::Right>, Error> {
        routine::TrySend::<T::Right, biunion::Right>::try_send(routine, data)
    }
}

/// One input side's borrow of the shared state.
pub(super) struct Feed<'a, T: Types, S: InputSide<T>> {
    inlet: &'a mut S::Inlet,
    /// This side's own waker.
    waker: &'a ThreadLocalWaker,
    /// The opposite input, woken on close.
    other: &'a ThreadLocalWaker,
    work: &'a mut Phase<T::Routine>,
    output: &'a ThreadLocalWaker,
    state: &'a mut NodeState<T::Signal>,
}

impl<T: Types, S: Feeding<T>> Feed<'_, T, S> {
    /// Feed the routine, held message first. Each stop has a waker: empty edge → upstream push;
    /// signal → Output on return to Running; refusal → routine; [BUDGET] → this side itself;
    /// closed → none (CloseReady).
    fn drain(mut self, held: &mut Option<S::Data>) -> Result<(), Error> {
        if self.retry(held)?.is_break() {
            return Ok(());
        }
        for _ in 0..BUDGET {
            if self.recv(held)?.is_break() {
                return Ok(());
            }
        }
        // Budget spent; the edge may hold more, so wake ourselves.
        self.waker.wake();
        Ok(())
    }

    /// Retry the held message: nothing may overtake it.
    fn retry(&mut self, held: &mut Option<S::Data>) -> Result<ControlFlow<()>, Error> {
        let Some(data) = held.take() else {
            return Ok(Continue(()));
        };
        let Some(data) = self.offer(data)? else {
            return Ok(Continue(()));
        };
        self.hold(data, held)?;
        Ok(Break(()))
    }

    /// Take one message from the edge.
    fn recv(&mut self, held: &mut Option<S::Data>) -> Result<ControlFlow<()>, Error> {
        match self.inlet.try_recv() {
            Ok(Some(Message::Data(data))) => match self.offer(data)? {
                None => Ok(Continue(())),
                Some(data) => {
                    self.hold(data, held)?;
                    Ok(Break(()))
                }
            },
            Ok(Some(Message::Flush(signal))) => {
                self.flushing(signal)?;
                Ok(Break(()))
            }
            Ok(Some(Message::Marker(signal))) => {
                self.marker_ready(signal);
                Ok(Break(()))
            }
            Ok(None) => Ok(Break(())),
            Err(e) if matches!(e.kind, ErrorKind::Closed) => {
                self.close();
                Ok(Break(()))
            }
            Err(e) => Err(e),
        }
    }

    /// Hold a refused message until the routine wakes this side. A closed edge can't be waited
    /// out: its close may have woken us already, so fast close drops the message instead.
    fn hold(&mut self, data: S::Data, held: &mut Option<S::Data>) -> Result<(), Error> {
        if self.inlet.closed()? {
            self.close();
        } else {
            *held = Some(data);
        }
        Ok(())
    }

    /// Offer data to the routine; the message comes back if it is full. `Closed` from a routine is
    /// fatal: routines handle close, never propagate it.
    fn offer(&mut self, data: S::Data) -> Result<Option<S::Data>, Error> {
        match S::try_send(&mut self.work.target, data) {
            Ok(Accept::Accepted) => Ok(None),
            Ok(Accept::Full(data)) => Ok(Some(data)),
            Err(e) if matches!(e.kind, ErrorKind::Closed) => Err(fatal!(
                "routine returned Closed from try_send — routines must handle close, not propagate it"
            )),
            Err(e) => Err(e),
        }
    }

    fn flushing(&mut self, signal: T::Signal) -> Result<(), Error> {
        self.work.target.flush()?;
        *self.state = NodeState::Flushing(signal);
        // Work must poll routine until Ready to complete the flush.
        self.work.waker.wake();
        Ok(())
    }

    fn marker_ready(&mut self, signal: T::Signal) {
        *self.state = NodeState::MarkerReady(signal);
        // Work gets one poll cycle before Output forwards the marker.
        self.work.waker.wake();
    }

    /// Fast close: skip flush, skip routine polling. Output drains buffered next() and closes the
    /// downstream edge; the routine drops with the node, and a refused message with it. Wake Work
    /// so it observes CloseReady and reports closed back to the poll loop; wake Output to drive
    /// the drain + close; wake the other input so it sees CloseReady and returns Closed.
    fn close(&mut self) {
        *self.state = NodeState::CloseReady;
        self.work.waker.wake();
        self.output.wake();
        self.other.wake();
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

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                S::feed(&mut s).drain(&mut self.held)?;
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
