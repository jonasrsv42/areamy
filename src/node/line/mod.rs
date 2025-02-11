//! Nodes to make line graphs. One input and one output, simple as it should be.
pub mod nosync;
mod reader;
mod routine;
pub mod sync;

pub use reader::LineReader;
pub use routine::{LineRoutine, Resume};
