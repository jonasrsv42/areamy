//! Graph signal passing for synchronization and message passing.
mod origin;
mod trackable;

pub use origin::Origin;
pub use trackable::{Trackable, Visitors};
