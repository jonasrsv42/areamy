use crate::error::Error;
use crate::graph::marker::Connection;
use crate::poll::waker::Waker;

/// A [`Room`] reports whether an outlet has room: readiness, not a push.
pub trait Room: Connection {
    /// `Ready` if there is room now; a hint, since another producer may take it first
    /// (the next [crate::graph::TryPushable::try_push] then refuses: poll again).
    /// Otherwise registers `waker`, woken on the next pop or close, and returns `Pending`.
    /// `Err(Closed)` when closed.
    fn poll(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error>;
}
