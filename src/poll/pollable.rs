use crate::error::Error;
use crate::graph::marker::Connection;
use crate::poll::Context;
use crate::thread::ThreadId;

/// [`Pollable`] is a [Connection] for event-driven nodes.
///
/// Unlike [crate::work::Workable] which blocks until work is done, [Pollable::poll] is non-blocking
/// and receives the node's [Context]: its wakers and its timer. A poll means the node may have
/// work, not that it was woken: see [wake order](crate::poll#wake-order).
///
/// Like [crate::work::Workable], [Pollable] has an associated [Pollable::ThreadId] to ensure
/// nodes are only added to matching threads (compile-time safety).
///
/// Not [crate::node::routine::Poll]: that is the user routine layer, where `Closed` is fatal.
/// Here `Closed` is control flow and, like `Ready`, drops the node.
pub trait Pollable: Connection {
    type ThreadId: ThreadId;
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error>;
}
