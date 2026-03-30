//! Future-based async routine primitives.
//!
//! [FutureRoutine] wraps a user-provided async fn into an
//! [AsyncLineRoutine](crate::AsyncLineRoutine). The async fn receives
//! input/output [Queue]s and drives I/O via async/await.
//!
//! For concurrent sub-tasks (bidi writer + reader), use
//! [Join](super::Join) or [Select](super::Select) inside the future.

pub mod queue;
pub mod routine;

pub use queue::{Queue, RecvFut};
pub use routine::{FutureRoutine, Input};
