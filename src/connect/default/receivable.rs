use crate::Receivable;
use crate::closed;
use crate::error::{Error, ErrorKind};
use crate::message::Message;
use std::cell::RefCell;
use std::rc::Rc;

impl<T: Receivable> Receivable for Rc<RefCell<T>> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;

    fn try_recv(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error> {
        self.borrow_mut().try_recv()
    }
}

/// Fan-in: drains from all inputs, closed only when ALL are closed.
impl<T: Receivable> Receivable for Vec<T> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;

    fn try_recv(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error> {
        let mut all_closed = true;
        for input in self.iter_mut() {
            match input.try_recv() {
                Ok(Some(msg)) => return Ok(Some(msg)),
                Ok(None) => all_closed = false,
                Err(e) if matches!(e.kind, ErrorKind::Closed) => continue,
                Err(e) => return Err(e),
            }
        }
        if all_closed { Err(closed!()) } else { Ok(None) }
    }
}
