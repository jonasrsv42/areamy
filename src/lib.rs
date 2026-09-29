// Graph types are deep by nature.
#![allow(clippy::type_complexity)]

extern crate alloc;

pub mod edge;
pub mod error;
pub mod graph;
pub mod message;
pub mod node;
pub mod reader;
mod signal;
pub mod thread;
pub mod typing;
pub mod work;
pub mod writer;
pub use pull::PullWriter;

pub mod pull;

pub use typing::{Combine, Composable, Contains, Decomposable, Generates};

pub use edge::policy::{PolicyEdge, SignalPolicy};
pub use edge::push::Push;
pub use edge::sync;
pub use graph::marker::{self, Connection};
pub use graph::{At, Closeable, Pushable, Receivable, TryPush, TryPushable};
pub use message::Message;
pub use node::{
    BifurcationIo, BifurcationRoutine, BiunionIo, BiunionRoutine, Flush, LineIo, LineRoutine, Next,
    Poll, Send,
};
pub use node::{bifurcation, biunion};
pub use poll::Pollable;
pub use pull::Pullable;
pub use signal::{Origin, Trackable};
pub use work::{ThreadStream, ThreadStreamHandle, Workable};

pub mod poll;
pub use reader::Reader;
pub use thread::{DefaultThread, ThreadBundle, ThreadBundleHandle, ThreadId};

#[cfg(test)]
pub mod tests;
