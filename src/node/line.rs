//! A variant of [crate::work::Workable], [crate::pull::Pullable], and [crate::poll::Pollable] nodes with one input and output.
mod io;
pub mod poll;
pub mod pull;
pub(crate) mod routine;
pub mod work;

pub use io::LineIo;
pub use routine::LineRoutine;
