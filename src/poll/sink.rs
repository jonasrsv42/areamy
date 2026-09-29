use crate::graph;
use crate::poll::Room;

/// A poll [`Sink`]: a [graph::Sink] that can also report [Room], so a poll node waits for room
/// instead of blocking its thread.
pub trait Sink: graph::Sink + Room {}
impl<T: graph::Sink + Room> Sink for T {}
