//! Cross-thread input into a poll node: a [`waking`](crate::edge::waking) edge whose consumer
//! waker is the node's Input phase.

pub use crate::edge::waking::{Receiver, Sender};
use crate::poll::wakers::allocator::Slot;
use crate::signal::Origin;
use std::task::Waker;

/// Sync input edge paired with its pre-allocated waker slot.
/// Created during `.input()` on the main thread.
pub struct Input<DataType, SignalType: Origin> {
    pub edge: Receiver<DataType, SignalType>,
    pub slot: Slot<Waker>,
}
