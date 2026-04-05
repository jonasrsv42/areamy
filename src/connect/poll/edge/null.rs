//! No-op sink. Discards all data pushed to it.
//!
//! Used as output for sink nodes (no downstream consumers) and as
//! placeholder storage for [`Deferred`](super::traits::Deferred) edges.

use crate::marker::Connection;
use crate::signal::Origin;

/// No-op sink. Discards all data pushed to it.
pub struct Null<DataType, SignalType: Origin>(
    std::marker::PhantomData<fn() -> (DataType, SignalType)>,
);

impl<DataType, SignalType: Origin> Null<DataType, SignalType> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<DataType, SignalType: Origin> Default for Null<DataType, SignalType> {
    fn default() -> Self {
        Self::new()
    }
}

impl<DataType, SignalType: Origin> Connection for Null<DataType, SignalType> {}

impl<DataType, SignalType: Origin> crate::Pushable for Null<DataType, SignalType> {
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(
        &mut self,
        _msg: crate::message::Message<DataType, SignalType>,
    ) -> Result<(), crate::error::Error> {
        Ok(())
    }
}

impl<DataType, SignalType: Origin> crate::Closeable for Null<DataType, SignalType> {
    fn close(&mut self) -> Result<(), crate::error::Error> {
        Ok(())
    }
}
