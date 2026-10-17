//! Concrete edges shared across paradigms.

pub(crate) mod deadlock;
pub mod fanout;
pub mod policy;
mod pushes;
pub mod rotate;
pub mod single;
pub mod sync;
pub mod waking;
