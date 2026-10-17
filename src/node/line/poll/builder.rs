//! Unified async node builder parameterized by edge kind markers.
//!
//! Use [`Node`](node::Node) via [`Thread::line`](crate::poll::Thread::line), or
//! `thread.builder().line()` to bound its inputs or set its [`Budget`](crate::poll::Budget)
//! first:
//!
//! - `.input::<Sync>()` → resolve input to Sync
//! - `.output::<Sync>()` → resolve output to Sync
//! - `up.via(node)` → Async input (adds parent `up`)
//! - `thread.add(node)` → Sync output (Spawnable)
//! - consumed by `node.via(down)` → Async output (AsyncParent)

pub mod node;
pub mod options;
pub mod traits;

#[cfg(test)]
mod tests;
