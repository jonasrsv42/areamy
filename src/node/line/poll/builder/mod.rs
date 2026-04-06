//! Unified async node builder parameterized by edge kind markers.
//!
//! Use [`Node`] via [`PollThread::line`](crate::PollThread):
//!
//! - `.typed::<OutEdge>()` → Sync input, explicit output
//! - `.parent(node)` → Async input (adds parent)
//! - `thread.add(node)` → Sync output (Spawnable)
//! - consumed by `.parent()` → Async output (AsyncParent)

pub mod node;
