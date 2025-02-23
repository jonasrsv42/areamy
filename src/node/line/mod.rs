//! A variant of [crate::Workable] and [crate::Pullable] nodes with one input and output.
pub mod pull;
mod reader;
mod routine;
pub mod work;

pub use reader::LineReader;
pub use routine::LineRoutine;
