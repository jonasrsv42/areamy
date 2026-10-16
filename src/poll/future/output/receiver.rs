use super::shared::Shared;
use crate::error::Error;
use std::rc::Rc;

/// Pops from the queue. Held by the node.
pub struct Receiver<T>(pub(super) Rc<Shared<T>>);

impl<T> Receiver<T> {
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
        awaking.wake(&self.0.home);
        Ok(Some(item))
    }
}
