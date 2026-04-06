//! A variant of [crate::Workable], [crate::Pullable], and [crate::Pollable] nodes with one input and output.
pub mod poll;
pub mod pull;
mod reader;
pub(crate) mod routine;
pub mod work;

pub use reader::LineReader;
pub use routine::LineRoutine;
