//! Unified async node builder parameterized by edge kind markers.
//!
//! Use [`Node`] via [`AsyncThread::node`](crate::AsyncThread):
//!
//! - `.typed::<OutEdge>()` → Sync input, explicit output
//! - `.parent(node)` → Async input (adds parent)
//! - `thread.add(node)` → Sync output (Spawnable)
//! - consumed by `.parent()` → Async output (Linkable<Parent>)

pub mod node;
