use crate::Receivable;
use crate::error::Error;
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
