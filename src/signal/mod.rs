//! Signals for synchronization and message passing in [crate::graph].
mod origin;
mod trackable;

pub use origin::Origin;
pub use trackable::{Trackable, Visitors};
