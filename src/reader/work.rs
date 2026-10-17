//! Work readers: [Reads] owns and drives a node, [Tees] reads one something else owns.

mod reader;
pub mod tee;

pub use reader::{Reader, Reads};
pub use tee::Tees;
