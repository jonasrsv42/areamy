use super::{NodeState, Shared, SharedState, Types};
use crate::error::{Error, ErrorKind};
use crate::graph::Receivable;
use crate::graph::marker::{Connection, Multiplicity};
use crate::message::Message;
use crate::node::{biunion, routine};
use crate::poll::waker::{ThreadLocalWaker, Waker};
use crate::poll::{Phase, Pollable};
use crate::signal::Origin;

impl<T: Types> SharedState<T> {
    fn drain_left(&mut self) -> Result<(), Error> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = self;
        drain(
            biunion::Left,
            &mut input.left.target,
            &input.right.waker,
            work,
            &output.waker,
            state,
        )
    }

    fn drain_right(&mut self) -> Result<(), Error> {
        let SharedState {
            input,
            work,
            output,
            state,
        } = self;
        drain(
            biunion::Right,
            &mut input.right.target,
            &input.left.waker,
            work,
            &output.waker,
            state,
        )
    }
}

/// Drain one side's inlet into the routine as `Side`. May transition state to Flushing,
/// MarkerReady or CloseReady. `other` is the opposite input, woken on close.
fn drain<Data, SignalType, Side, Inlet, RoutineType>(
    _side: Side,
    inlet: &mut Inlet,
    other: &ThreadLocalWaker,
    work: &mut Phase<RoutineType>,
    output: &ThreadLocalWaker,
    state: &mut NodeState<SignalType>,
) -> Result<(), Error>
where
    SignalType: Origin,
    Side: Multiplicity,
    Inlet: Receivable<DataType = Data, SignalType = SignalType>,
    RoutineType: routine::Send<Data, Side> + routine::Flush,
{
    loop {
        match inlet.try_recv() {
            Ok(Some(Message::Data(data))) => {
                work.target.send(data)?;
            }
            Ok(Some(Message::Flush(signal))) => {
                work.target.flush()?;
                *state = NodeState::Flushing(signal);
                // Work must poll routine until Ready to complete the flush.
                work.waker.wake();
                break;
            }
            Ok(Some(Message::Marker(signal))) => {
                *state = NodeState::MarkerReady(signal);
                // Work gets one poll cycle before Output forwards the marker.
                work.waker.wake();
                break;
            }
            Ok(None) => break,
            Err(e) => {
                if matches!(e.kind, ErrorKind::Closed) {
                    // Fast close: skip flush, skip routine polling.
                    // Output drains buffered next() and closes the
                    // downstream edge; routine drops with the node.
                    *state = NodeState::CloseReady;
                    work.waker.wake();
                    output.wake();
                    // Wake the other input so it sees CloseReady and
                    // returns Closed.
                    other.wake();
                    break;
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

pub struct LeftInput<T: Types> {
    shared: Shared<T>,
}

impl<T: Types> LeftInput<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }
}

impl<T: Types> Connection for LeftInput<T> {}

impl<T: Types> Pollable for LeftInput<T> {
    type ThreadId = T::ThreadId;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_left()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            _ => Ok(core::task::Poll::Pending),
        }
    }
}

pub struct RightInput<T: Types> {
    shared: Shared<T>,
}

impl<T: Types> RightInput<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }
}

impl<T: Types> Connection for RightInput<T> {}

impl<T: Types> Pollable for RightInput<T> {
    type ThreadId = T::ThreadId;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_right()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            _ => Ok(core::task::Poll::Pending),
        }
    }
}
