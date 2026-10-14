use super::{NodeState, Shared, SharedState, Types};
use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::graph::marker::Connection;
use crate::node::routine;
use crate::poll::Pollable;
use crate::poll::waker::Waker;

impl<T: Types> SharedState<T> {
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

pub struct Work<T: Types> {
    shared: Shared<T>,
}

impl<T: Types> Work<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }
}

impl<T: Types> Connection for Work<T> {}

impl<T: Types> Pollable for Work<T> {
    type ThreadId = T::ThreadId;

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
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                s.output.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::FlushReady(_) => Ok(core::task::Poll::Pending),
        }
    }
}
