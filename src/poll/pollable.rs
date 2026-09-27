use crate::ThreadId;
use crate::connect::waker::Waker;
use crate::error::Error;
use crate::graph::marker::Connection;

/// [`Pollable`] is a [Connection] for event-driven nodes.
///
/// Unlike [crate::work::Workable] which blocks until work is done, [Pollable::poll] is non-blocking
/// and receives a [Waker] carrying both a sync waker
/// (for I/O registration / standard futures) and a thread-local waker (for cheap
/// same-thread wake).
///
/// Like [crate::work::Workable], [Pollable] has an associated [Pollable::ThreadId] to ensure
/// nodes are only added to matching threads (compile-time safety).
pub trait Pollable: Connection {
    type ThreadId: ThreadId;
    fn poll(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error>;
}
