use super::{NodeState, Shared, SharedState, Types};
use crate::error::{Error, ErrorKind};
use crate::graph::Receivable;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::node::routine::{Flush as _, Send as _};
use crate::poll::Pollable;
use crate::poll::waker::Waker;

impl<T: Types> SharedState<T> {
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
pub struct Input<T: Types> {
    pub(crate) shared: Shared<T>,
}

impl<T: Types> Input<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
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
