use super::shared::Shared;
use super::{Input, Receiver};
use crate::error::Error;
use crate::fatal;
use crate::node::routine::Accept;
use std::rc::Rc;

/// Sends into the queue. Held by the node.
pub struct Sender<T>(pub(super) Rc<Shared<T>>);

impl<T> Sender<T> {
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
        awaking.wake(&self.0.home);
        Ok(Accept::Accepted)
    }

    /// May land one past the bound.
    pub fn flush(&self) -> Result<(), Error> {
        let awaking = {
            let mut inner = self.0.borrow()?;
            inner.buffer.push_back(Input::Flush);
            inner.waiters.take()
        };
        awaking.wake(&self.0.home);
        Ok(())
    }

    /// Checks a segment ended cleanly: errors if the future didn't fully drain or didn't observe
    /// the Flush — both are routine bugs.
    pub fn ended(&self) -> Result<(), Error> {
        let inner = self.0.borrow()?;
        if !inner.buffer.is_empty() {
            return Err(fatal!(
                "input queue still has items at segment end — future returned without draining input"
            ));
        }
        if !inner.closed {
            return Err(fatal!(
                "input queue not closed at segment end — future returned without receiving Flush"
            ));
        }
        Ok(())
    }

    /// A receiver of the current segment's queue.
    pub fn receiver(&self) -> Receiver<T> {
        Receiver(self.0.clone())
    }

    /// Ends a segment ([Self::ended]), then swaps in a fresh queue and returns its receiver, for
    /// the next future. A receiver kept from the old segment stays closed and never sees the next
    /// one's data; with none kept, the old queue is reset and reused instead.
    pub fn renew(&mut self) -> Result<Receiver<T>, Error> {
        self.ended()?;
        // No old receiver left to see the reset: reuse the block and its buffer.
        match Rc::get_mut(&mut self.0) {
            Some(shared) => shared.reset(),
            None => {
                let shared = &self.0;
                self.0 = Rc::new(Shared::new(
                    shared.input.clone(),
                    shared.home.clone(),
                    shared.timer.clone(),
                    shared.bound,
                ));
            }
        }
        Ok(Receiver(self.0.clone()))
    }
}
