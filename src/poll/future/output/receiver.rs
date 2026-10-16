use super::shared::Shared;
use crate::error::Error;
use crate::wake;
use std::rc::Rc;

/// Pops from the queue. Held by the node.
pub struct Receiver<T>(pub(super) Rc<Shared<T>>);

impl<T> Receiver<T> {
    /// The node's waker, from the routine's first poll; the first call wins.
    pub fn home(&self, wake: &wake::Waker) -> Result<(), Error> {
        self.0.borrow()?.waiters.home(wake);
        Ok(())
    }

    /// Next item; a pop that frees room wakes every waiting send, after the borrow.
    pub fn pop(&self) -> Result<Option<T>, Error> {
        let (item, awaking) = {
            let mut inner = self.0.borrow()?;
            let Some(item) = inner.buffer.pop_front() else {
                inner.armed = true;
                return Ok(None);
            };
            (item, inner.waiters.take())
        };
        awaking.wake();
        Ok(Some(item))
    }
}
