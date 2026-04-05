//! [Producer] — cross-thread producer. `Send + Sync + Clone`.

use super::core::VyukovQueue;
use crate::connect::poll::marker::NodeId;
use crate::error::Error;

use alloc::sync::Arc;

/// Cross-thread producer. `Send + Sync + Clone`.
/// Enqueues an item and signals the consumer.
#[derive(Clone)]
pub struct Producer {
    pub(super) inner: Arc<VyukovQueue>,
}

impl Producer {
    /// Enqueue an item and wake the consumer.
    pub fn push(&self, id: NodeId) -> Result<(), Error> {
        self.inner.push(id);
        self.inner.signal()
    }
}
