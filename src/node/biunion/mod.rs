//! A [crate::Workable] connection with two inputs (Experimental).

mod marker;
pub mod poll;
mod reader;
mod routine;
pub mod work;
pub use marker::{Left, Right, Side};

pub use reader::BiunionReader;
pub use routine::BiunionRoutine;
