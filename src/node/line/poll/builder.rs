//! Unified async node builder parameterized by edge kind markers.
//!
//! Use [`Node`](node::Node) via [`Thread::line`](crate::poll::Thread::line), or
//! `thread.builder().line()` to bound its inputs or set its [`Budget`](crate::poll::Budget)
//! first:
//!
//! - `.input()` → resolve input to Sync
//! - `.output()` → resolve output to Sync
//! - `up.via(node)` → Async input (adds parent `up`)
//! - `thread.add(node)` → Sync output (GraphBuilder)
//! - consumed by `node.via(down)` → Async output (AsyncParent)

pub mod node;
pub mod options;
pub mod traits;
pub mod types;

#[cfg(test)]
mod tests;
