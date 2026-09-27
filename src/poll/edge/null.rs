//! No-op sink. Discards all data pushed to it.
//!
//! Used as output for sink nodes (no downstream consumers) and as
//! placeholder storage for [`Deferred`](super::traits::Deferred) edges.

use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Pushable};
use crate::message::Message;
use crate::signal::Origin;
use std::marker::PhantomData;

/// No-op sink. Discards all data pushed to it.
pub struct Null<DataType, SignalType: Origin>(PhantomData<fn() -> (DataType, SignalType)>);

impl<DataType, SignalType: Origin> Null<DataType, SignalType> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<DataType, SignalType: Origin> Default for Null<DataType, SignalType> {
    fn default() -> Self {
        Self::new()
    }
}

impl<DataType, SignalType: Origin> Connection for Null<DataType, SignalType> {}

impl<DataType, SignalType: Origin> Pushable for Null<DataType, SignalType> {
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, _msg: Message<DataType, SignalType>) -> Result<(), Error> {
        Ok(())
    }
}

impl<DataType, SignalType: Origin> Closeable for Null<DataType, SignalType> {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
