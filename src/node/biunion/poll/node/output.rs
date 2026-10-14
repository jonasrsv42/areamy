use super::{NodeState, Shared, SharedState, Types};
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Pushable};
use crate::message::Message;
use crate::node::routine::Next as _;
use crate::poll::Pollable;
use crate::poll::waker::Waker;

impl<T: Types> SharedState<T> {
    fn push_output(&mut self, msg: Message<T::Out, T::Signal>) -> Result<(), Error> {
        self.output.target.push(msg)
    }

    fn drain_output(&mut self) -> Result<(), Error> {
        while let Some(out) = self.work.target.next()? {
            self.push_output(Message::Data(out))?;
        }
        Ok(())
    }
}

pub struct Output<T: Types> {
    shared: Shared<T>,
}

impl<T: Types> Output<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }
}

impl<T: Types> Connection for Output<T> {}

impl<T: Types> Pollable for Output<T> {
    type ThreadId = T::ThreadId;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed => Err(crate::closed!()),
            NodeState::FlushReady(_) => {
                s.drain_output()?;
                let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                    NodeState::FlushReady(sig) => sig,
                    _ => return Err(fatal!("expected FlushReady state")),
                };
                s.push_output(Message::Flush(signal))?;
                // Flush forwarded, back to Running. Both inputs can resume.
                s.input.left.waker.wake();
                s.input.right.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                s.drain_output()?;
                let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                    NodeState::MarkerReady(sig) => sig,
                    _ => return Err(fatal!("expected MarkerReady state")),
                };
                s.push_output(Message::Marker(signal))?;
                // Marker forwarded, back to Running. Both inputs can resume.
                s.input.left.waker.wake();
                s.input.right.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::CloseReady => {
                s.drain_output()?;
                s.output.target.close()?;
                s.state = NodeState::Closed;
                Err(crate::closed!())
            }
            _ => {
                s.drain_output()?;
                Ok(core::task::Poll::Pending)
            }
        }
    }
}
