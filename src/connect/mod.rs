//!
//! Basic building blocks for for declaring and running multithreaded streaming graphs.
//!
//! See [graph] for the core of Areamy. Areamy provides a set of [marker::Connection] for
//! building [std::marker::Sync] graphs where scheduling can be a connection such as [Workable] and dataflow
//! can happen through [Pushable] edges.
//!
//! Since standard scheduling of work is handled through [Workable] edges
//! scheduling becomes a depth first search through the graph towards data. The idea is that we
//! find the shortest path to produce some output. This ***Can*** be useful for latency but can be
//! harmful for throughput. Which is good to be aware of when choosing to use Areamy.
//!
//! We support building hybrid graphs where certain parts are [std::marker::Sync] but others
//! are only [std::marker::Send] where those parts can be lock and dispatch free for performance.
//!
//! The intended use-case for areamy is to support building various on-device streaming machine learning
//! pipelines where each node may be running some data preprocessing, post-processing or model inference
//! and everything is being executed in a low latency streaming fashion.
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
//! For code see source in [graph::tests]
//!
//! Areamy [std::marker::Sync] graphs make liberal use of [Dynamic Dispatch](https://doc.rust-lang.org/1.8.0/book/trait-objects.html#dynamic-dispatch) to
//!
//! 1. Allow [marker::Connection]s of different types to a single node. Many nodes will store a
//!    list of outbound dyn [Pushable] and inbound dyn [Workable] that could be different types
//!    of [Workable] and [Pushable]. Dynamic dispatch simplifies this.
//! 2. **Improve error messages**. statically dispatched connections will require templating and
//!    the template nesting will grow with the size of the graph causing horrendous type defintions
//!    that causes a headache to read.
//!
//! For [std::marker::Sync] graphs the cost of dynamic dispatch is considered negligable. However,
//! if for some reason a subgraph needs to be as performant as possible, areamy provides a way to
//! build a [Pullable] subgraph that avoids dynamic dispatch and all synchronization primitives. See [crate::pull::Line]
//!

mod default;
pub mod graph;
mod make;
pub mod marker;
pub mod poll;
pub mod signal_policy;
pub mod sync_edge;
pub mod waker;

pub use graph::{Closeable, Pollable, Pullable, Pushable, Receivable, Workable};
pub use make::work;
pub use make::{make_bidi, make_push, make_work};
pub use poll::{PollEdge, SyncBridge};
pub use signal_policy::{PolicyEdge, SignalPolicy};
pub use sync_edge::SyncEdge;
