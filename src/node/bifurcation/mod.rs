//! A [crate::Workable] connection with two outputs (Experimental). 

mod marker;
mod reader;
pub mod routine;
pub mod sync;

pub use marker::{Left, Right};
pub use reader::BifurcationReader;
pub use routine::BifurcationRoutine;
