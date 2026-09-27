use crate::ThreadId;
use crate::error::Error;
use crate::graph::marker::Connection;

mod default;

/// A [`Workable`] is a [Connection] in our graph that is used to for scheduling.
/// Child nodes will invoke the [Workable] connection to parents to make parent work.
///
/// The [Workable] has an associated [Workable::ThreadId] to indicate the unique thread that
/// is allowed to [Workable::work] on this node. By marking nodes we can ensure we don't have
/// threads contending on nodes.
pub trait Workable: Send + Connection {
    // Thread associated with this `Workable`.
    type ThreadId: ThreadId;
    fn work(&mut self) -> Result<(), Error>;
}
