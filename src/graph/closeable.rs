use crate::error::Error;
use crate::graph::marker::Connection;

mod default;

/// A [`Closeable`] can be closed to signal no more data will be sent.
///
/// When a node receives an error from upstream (via pull/work connections), it should
/// close all its [crate::work::Sink] outputs to propagate the shutdown through push connections.
/// This enables clean shutdown cascade through the entire graph.
///
/// The exact semantics of close (e.g., first-close-wins vs refcounted) are up to the
/// implementation.
pub trait Closeable: Connection {
    /// Close this connection, signaling no more data will be sent.
    fn close(&mut self) -> Result<(), Error>;
}
