//! Future-based async routine primitives.
//!
//! - [line::FutureRoutine] — single input, single output
//! - [biunion::FutureRoutine] — two inputs, single output
//!
//! For concurrent sub-tasks, use [try_join](super::try_join) or
//! [race](super::race).

pub mod biunion;
pub mod line;
pub mod queue;

pub use queue::{Input, InputConsumer, InputQueue, OutputProducer, OutputQueue, RecvFut};
