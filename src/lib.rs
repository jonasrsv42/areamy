//! Streaming graphs whose wiring is checked at compile time.
//!
//! Wire nodes with the verbs: [Via] owns an upstream, [Pushes] pushes data only, [Schedules]
//! schedules only; [At] names a side. Endpoints: [Writes], [Reads], [Tees], [Streams],
//! [Expects]. Three paradigms: [work] (blocking), [poll] (async) and [pull].

// Graph types are deep by nature.
#![allow(clippy::type_complexity)]

extern crate alloc;

pub mod edge;
pub mod error;
pub mod graph;
pub mod message;
pub mod node;
pub mod poll;
pub mod pull;
pub mod reader;
mod signal;
pub mod thread;
pub mod typing;
pub mod wake;
pub mod work;
pub mod writer;

pub use typing::{Combine, Composable, Contains, Decomposable, Generates};

pub use edge::sync;
pub use graph::marker::{self, Connection};
pub use graph::{At, Pushes, Via};
pub use graph::{Closeable, Inlet, Outlet, Pushable, Receivable, TryPush, TryPushable};
pub use message::Message;
pub use node::{
    Accept, BifurcationIo, BifurcationRoutine, BiunionIo, BiunionRoutine, Flush, LineIo,
    LineRoutine, Next, Poll, Send, TrySend,
};
pub use node::{bifurcation, biunion};
pub use poll::Pollable;
pub use pull::{PullWriter, Pullable};
pub use reader::Reader;
pub use signal::{Origin, Trackable};
pub use thread::{DefaultThread, ThreadBundle, ThreadBundleHandle, ThreadId};
pub use work::{Expects, Reads, Schedules, Streams, Tees, Writes};
pub use work::{ThreadStream, ThreadStreamHandle, Workable};

#[cfg(test)]
pub mod tests;
