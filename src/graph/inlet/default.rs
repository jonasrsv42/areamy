use crate::graph::Inlet;

impl<T: Inlet> Inlet for Vec<T> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;
}
