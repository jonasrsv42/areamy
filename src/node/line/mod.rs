//! Nodes to make line graphs. One input and one output, simple as it should be.
pub mod pull;
mod reader;
mod routine;
pub mod work;

pub use reader::LineReader;
pub use routine::LineRoutine;
