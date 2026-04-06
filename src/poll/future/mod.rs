//! Future-based async routine primitives.
//!
//! - [line::FutureRoutine] — single input, single output
//! - [biunion::FutureRoutine] — two inputs, single output
//!
//! For concurrent sub-tasks, use [Join](super::Join) or [Select](super::Select).

pub mod biunion;
pub mod line;
pub mod queue;

pub use queue::{Input, InputConsumer, InputQueue, OutputProducer, OutputQueue, RecvFut};
