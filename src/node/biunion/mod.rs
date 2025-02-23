//! A [crate::Workable] connection with two inputs (Experimental).

mod marker;
mod reader;
mod routine;
pub mod work;
pub use marker::{Left, Right};

pub use reader::BiunionReader;
pub use routine::BiunionRoutine;
