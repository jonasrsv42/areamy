//! Future-based async routine primitives.
//!
//! [FutureRoutine] wraps a user-provided async fn into an
//! [PollLineRoutine](crate::PollLineRoutine). The async fn receives
//! an [InputConsumer] and [OutputProducer] and drives I/O via async/await.
//!
//! For concurrent sub-tasks (bidi writer + reader), use
//! [Join](super::Join) or [Select](super::Select) inside the future.

pub mod queue;
pub mod routine;

pub use queue::{Input, InputConsumer, InputQueue, OutputProducer, OutputQueue, RecvFut};
pub use routine::FutureRoutine;
