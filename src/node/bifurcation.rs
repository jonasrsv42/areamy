//! A [crate::Workable] connection with two outputs (Experimental).

mod io;
mod marker;
pub mod routine;
pub mod work;

pub use io::BifurcationIo;
pub use marker::{Left, Right};
pub use routine::BifurcationRoutine;
