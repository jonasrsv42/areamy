//! Unified async node builder parameterized by edge kind markers.
//!
//! Use [`Node`](node::Node) via [`Thread::line`](crate::poll::Thread::line):
//!
//! - `.input::<Sync>()` → resolve input to Sync
//! - `.output::<Sync>()` → resolve output to Sync
//! - `.parent(node)` → Async input (adds parent)
//! - `thread.add(node)` → Sync output (Spawnable)
//! - consumed by `.parent()` → Async output (AsyncParent)

pub mod node;
pub mod traits;

#[cfg(test)]
mod tests;
