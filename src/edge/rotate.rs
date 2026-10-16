//! [Rotate]: a node's output, delivering one message at a time and holding a refusal.

use crate::edge::fanout::Rotation;
use crate::error::Error;
use crate::graph::{Closeable, Outlet, Schedule};
use crate::message::Message;

/// Delivers each message to a node's output edges without blocking: a refusal holds the message
/// until [Rotate::retry] delivers it, and nothing new may be rotated while one is held.
///
/// [Fanout](crate::edge::fanout::Fanout) serves several edges and so clones;
/// [Single](crate::edge::single::Single) serves one and never does.
pub trait Rotate: Outlet + Schedule + Closeable {
    /// The edge holding a message, if any.
    fn suspended(&self) -> Option<usize>;
    /// Deliver the held message; `Complete` once nothing is held.
    fn retry(&mut self) -> Result<Rotation, Error>;
    /// Deliver a new message. An error while one is held.
    fn rotate(
        &mut self,
        message: Message<Self::DataType, Self::SignalType>,
    ) -> Result<Rotation, Error>;
}
