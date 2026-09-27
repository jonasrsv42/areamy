//! A [crate::work::Workable] connection with two inputs (Experimental).

mod io;
mod marker;
pub mod poll;
pub(crate) mod routine;
pub mod work;
pub use marker::{Left, Right, Side};

pub use io::BiunionIo;
pub use routine::BiunionRoutine;
