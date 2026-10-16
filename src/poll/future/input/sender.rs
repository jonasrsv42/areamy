use super::shared::Shared;
use super::{Input, Receiver};
use crate::error::Error;
use crate::fatal;
use crate::node::routine::Accept;
use crate::wake;
use std::rc::Rc;

/// Sends into the queue. Held by the node.
pub struct Sender<T>(pub(super) Rc<Shared<T>>);

impl<T> Sender<T> {
    /// The node's waker, from the routine's first poll; the first call wins.
    pub fn home(&self, wake: &wake::Waker) -> Result<(), Error> {
        self.0.borrow()?.waiters.home(wake);
        Ok(())
    }

    /// `Full` at the bound, owing Input one wake once the future takes an item.
    pub fn try_send(&self, item: T) -> Result<Accept<T>, Error> {
        let awaking = {
            let mut inner = self.0.borrow()?;
            if self
                .0
                .bound
                .is_some_and(|bound| inner.buffer.len() >= bound.get())
            {
                inner.refused = true;
                return Ok(Accept::Full(item));
            }
            inner.buffer.push_back(Input::Data(item));
            inner.waiters.take()
        };
        awaking.wake();
        Ok(Accept::Accepted)
    }

    /// May land one past the bound.
    pub fn flush(&self) -> Result<(), Error> {
        let awaking = {
            let mut inner = self.0.borrow()?;
            inner.buffer.push_back(Input::Flush);
            inner.waiters.take()
        };
        awaking.wake();
        Ok(())
    }

    /// Ends a segment: swaps in a fresh queue and returns its receiver, for the next future.
    /// Errors if the future didn't fully drain or didn't observe the Flush — both are routine
    /// bugs.
    pub fn renew(&mut self) -> Result<Receiver<T>, Error> {
        let waiters = {
            let inner = self.0.borrow()?;
            if !inner.buffer.is_empty() {
                return Err(fatal!(
                    "input queue still has items on renew — future returned without draining input"
                ));
            }
            if !inner.closed {
                return Err(fatal!(
                    "input queue not closed on renew — future returned without receiving Flush"
                ));
            }
            inner.waiters.renewed()
        };
        let shared = &self.0;
        self.0 = Rc::new(Shared::new(
            shared.input.clone(),
            shared.timer.clone(),
            shared.bound,
            waiters,
        ));
        Ok(Receiver(self.0.clone()))
    }
}
