//! Signal policy for controlling how signals are processed by connections.

use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq)]
/// Policy for handling signals in the queue
pub enum Policy {
    /// Always forward signals into the queue
    Forward,
    /// Only forward signals if the last entry was not a signal
    FollowData,
    /// Never forward signals into the queue
    Block,
}

/// A trait for types that can have their signal policy configured.
pub trait SignalPolicy {
    /// Set the signal policy for this object
    fn set_signal_policy(&self, policy: Policy) -> Result<(), Error>;
}