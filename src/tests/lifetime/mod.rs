//! Lifetime cascade smoke tests.
//!
//! Each test builds a routine that borrows from a stack-allocated config,
//! adds it to a [`crate::ThreadBundle`], and runs it inside
//! `std::thread::scope`. This exercises the full storage cascade: the
//! borrow flows through routine → node → thread → bundle → scope, and
//! back through `join()`.
//!
//! Coverage matrix (per node-type × connection-trait):
//! - **line** × `Workable` / `Pollable` / `Pullable`
//! - **biunion** × `Workable` / `Pollable`
//! - **bifurcation** × `Workable`
//!
//! Topology extensions (one borrow shared across more than one node):
//! - Multi-thread bundle sharing `&config` (see [`work`])
//! - Async parent chain in poll (see [`poll`])
//! - Borrowed Sync poll output sink — exercises `Edge::Output<'params>` (see [`poll`])
//! - Borrowed parent pushing into a poll node via `make_push` —
//!   exercises `Get<dyn ... + 'params>` selection (see [`poll`])

mod mock;
mod poll;
mod pull;
mod sink;
mod source;
mod work;
