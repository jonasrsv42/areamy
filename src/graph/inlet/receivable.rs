use crate::error::Error;
use crate::graph::Inlet;
use crate::message::Message;

mod default;

/// [`Receivable`] is a non-blocking [Inlet].
///
/// It checks if data is available, without blocking,
/// and return is there is or [`None`] if there's none
///
/// In [`crate::poll::Pollable`] graphs we do not couple edges with scheduling any
/// non-awaitable edge is therefore [`Receivable`]
///
pub trait Receivable: Inlet {
    /// Non-blocking receive.
    /// Returns `Ok(Some(msg))` if data is available.
    /// Returns `Ok(None)` if open but empty.
    /// Returns `Err(Closed)` if closed and empty.
    fn try_recv(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error>;
}
