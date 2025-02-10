//!
//! Traits for declaring and building, optionally multi-threaded, streaming graphs.
//!
//! We provide [marker::Connection] for building `Sync` graphs such as [Workable] and [Pushable]
//! and [Pullable] for building graphs with less synchronization primitives.
//!
//! We support building hybrid graphs where certain parts are `Sync` but others
//! are guaranteed to only be `Send`.
//!
//! Example of multi-threaded hybrid graph with various [Pullable::pull], [Pushable::push] and [Workable::work]
//! [marker::Connection]
//!
//! ```bash
//!          (output)
//!         (thread 1)   (thread 2)
//!     (push) ↑ ↓ (work) ↗ (work)
//!            node node → →
//!     (push) ↑ ↗ (work)  ↓
//!            node        ↓
//!     (push) ↑ ↓ (work)  ↓
//!            node  ← ← ← ↓ (push)
//!             ↑  (pull)
//!            node
//!             ↑ (pull)
//!            node
//!           (input)
//! ```
//!
//! For code example see source in [graph::tests]

mod default;
pub mod graph;
mod make;
pub mod marker;

pub use default::pullable::NoPull;
pub use graph::{Pullable, Pushable, Workable};
pub use make::sync;
pub use make::{make_bidi, make_push, make_work};
