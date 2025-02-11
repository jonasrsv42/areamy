//!
//! A [crate::Pullable] connection well suited for making nosync [crate::LineRoutine] segments.
//!
//! nosync implying that it does not implement [std::marker::Sync] and thus requires less
//! synchronization primitives.
//!
mod builder;
mod node;
mod reader;

pub use builder::{make_pull, Connect, Root};
pub use node::Line;
pub use reader::read_until;
