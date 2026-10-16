//! Future-based async routine primitives.
//!
//! - [line::FutureRoutine] — single input, single output
//! - [biunion::FutureRoutine] — two inputs, single output
//!
//! For concurrent sub-tasks, use [try_join](fn@super::try_join) or
//! [race](fn@super::race).

pub mod biunion;
mod deadline;
pub mod input;
pub mod line;
pub mod output;
mod waiters;

pub use input::Input;
