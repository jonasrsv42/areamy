use crate::graph::Outlet;
use crate::signal::Origin;

impl<PushableType: ?Sized, DataType, SignalType> Outlet for Box<PushableType>
where
    SignalType: Origin,
    PushableType: Outlet<DataType = DataType, SignalType = SignalType>,
{
    type DataType = DataType;
    type SignalType = SignalType;
}
