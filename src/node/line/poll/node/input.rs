use super::{NodeState, Shared, SharedState, Types};
use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Closed, Receivable};
use crate::message::Message;
use crate::node::routine::{Accept, Flush as _, TrySend as _};
use crate::poll::waker::Waker;
use crate::poll::{BUDGET, Pollable};
use std::ops::ControlFlow::{self, Break, Continue};

impl<T: Types> SharedState<T> {
    /// Feed the routine, held message first. Each stop has a waker: empty edge → upstream push;
    /// signal → Output on return to Running; refusal → routine; [BUDGET] → Input itself;
    /// closed → none (CloseReady).
    fn drain_input(&mut self, held: &mut Option<T::In>) -> Result<(), Error> {
        if self.retry(held)?.is_break() {
            return Ok(());
        }
        for _ in 0..BUDGET {
            if self.recv(held)?.is_break() {
                return Ok(());
            }
        }
        // Budget spent; the edge may hold more, so wake ourselves.
        self.input.waker.wake();
        Ok(())
    }

    /// Retry the held message: nothing may overtake it.
    fn retry(&mut self, held: &mut Option<T::In>) -> Result<ControlFlow<()>, Error> {
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
    fn recv(&mut self, held: &mut Option<T::In>) -> Result<ControlFlow<()>, Error> {
        match self.input.target.try_recv() {
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

    /// Hold a refused message until the routine wakes Input. A closed edge can't be waited out:
    /// its close may have woken us already, so fast close drops the message instead.
    fn hold(&mut self, data: T::In, held: &mut Option<T::In>) -> Result<(), Error> {
        if self.input.target.closed()? {
            self.close();
        } else {
            *held = Some(data);
        }
        Ok(())
    }

    /// Offer data to the routine; the message comes back if it is full. `Closed` from a routine is
    /// fatal: routines handle close, never propagate it.
    fn offer(&mut self, data: T::In) -> Result<Option<T::In>, Error> {
        match self.work.target.try_send(data) {
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
        self.state = NodeState::Flushing(signal);
        // Work must poll routine until Ready to complete the flush.
        self.work.waker.wake();
        Ok(())
    }

    fn marker_ready(&mut self, signal: T::Signal) {
        self.state = NodeState::MarkerReady(signal);
        // Work gets one poll cycle to let routine emit pending output
        // before Output forwards the marker.
        self.work.waker.wake();
    }

    /// Fast close: skip flush, skip routine polling. Output drains buffered next() and closes the
    /// downstream edge; the routine drops with the node, and a refused message with it. Wake Work
    /// so it observes CloseReady and reports closed back to the poll loop; wake Output to drive
    /// the drain + close.
    fn close(&mut self) {
        self.state = NodeState::CloseReady;
        self.work.waker.wake();
        self.output.waker.wake();
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
                s.drain_input(&mut self.held)?;
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
