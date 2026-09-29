use crate::graph::Outlet;
use crate::signal::Origin;
use std::cell::RefCell;
use std::rc::Rc;

impl<T: Outlet> Outlet for Vec<T>
where
    T::DataType: Clone,
    T::SignalType: Origin + Clone,
{
    type DataType = T::DataType;
    type SignalType = T::SignalType;
}

impl<T: Outlet> Outlet for Rc<RefCell<T>> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;
}

impl<PushableType: ?Sized, DataType, SignalType> Outlet for Box<PushableType>
where
    SignalType: Origin,
    PushableType: Outlet<DataType = DataType, SignalType = SignalType>,
{
    type DataType = DataType;
    type SignalType = SignalType;
}
