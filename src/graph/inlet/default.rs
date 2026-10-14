use crate::graph::Inlet;
use std::cell::RefCell;
use std::rc::Rc;

impl<T: Inlet> Inlet for Rc<RefCell<T>> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;
}

impl<T: Inlet> Inlet for Vec<T> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;
}
