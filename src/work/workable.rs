use crate::error::Error;
use crate::graph::marker::Connection;
use crate::thread::ThreadId;

mod default;

/// A [`Workable`] is a [Connection] in our graph that is used to for scheduling.
/// A downstream works its upstreams through this.
///
/// The [Workable] has an associated [Workable::ThreadId] to indicate the unique thread that
/// is allowed to [Workable::work] on this node. By marking nodes we can ensure we don't have
/// threads contending on nodes.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a work node",
    note = "a poll node can't be owned by a work node: bridge with a Sync output (`.output()` + `pushes`)"
)]
pub trait Workable: Send + Connection {
    // Thread associated with this `Workable`.
    type ThreadId: ThreadId;
    fn work(&mut self) -> Result<(), Error>;
}
