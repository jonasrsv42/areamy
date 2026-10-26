//! Cross-thread input into a poll node: a [`waking`](crate::edge::waking) edge whose consumer
//! waker is the node's Input phase.

pub use crate::edge::waking::{Receiver, Sender};
use crate::poll::graph::Reserved;
use crate::signal::Origin;
use crate::thread::ThreadId;

/// Sync input edge paired with the node it wakes, reserved at mint.
pub struct Input<DataType, SignalType: Origin, ThreadIdType: ThreadId> {
    pub edge: Receiver<DataType, SignalType>,
    pub reserved: Reserved<ThreadIdType>,
}
