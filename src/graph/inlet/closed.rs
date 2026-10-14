use crate::error::Error;
use crate::graph::Inlet;

mod default;

/// [`Closed`] asks an [Inlet] whether its producers have closed, without taking anything out.
///
/// Unlike [crate::graph::Receivable::try_recv], which reports close only once drained, this is
/// true as soon as no more messages can arrive, whatever is still buffered.
pub trait Closed: Inlet {
    fn closed(&self) -> Result<bool, Error>;
}
