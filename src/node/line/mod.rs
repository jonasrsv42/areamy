//! A variant of [crate::Workable], [crate::Pullable], and [crate::Pollable] nodes with one input and output.
mod io;
pub mod poll;
pub mod pull;
pub(crate) mod routine;
pub mod work;

pub use io::LineIo;
pub use routine::LineRoutine;
