extern crate alloc;

mod combine;
pub mod composable;
pub mod connect;
pub mod error;
pub mod message;
pub mod node;
mod signal;
pub mod sink;
pub mod source;
mod thread;
pub mod work;
pub use source::source::{GraphPullSource, GraphPushSource};
mod contains;
mod generates;

pub mod pull;

pub use combine::Combine;
pub use composable::{Composable, Decomposable};
pub use connect::marker;
pub use contains::Contains;
pub use generates::Generates;

pub use crate::connect::graph;
pub use connect::{
    Closeable, PolicyEdge, Pollable, Pullable, Pushable, Receivable, SignalPolicy, SyncBridge,
    SyncEdge, Workable, make_bidi, make_push, make_work, marker::Connection,
};
pub use message::Message;
pub use node::{
    BifurcationReader, BifurcationRoutine, BiunionReader, BiunionRoutine, Flush, LineReader,
    LineRoutine, Next, Poll, Send,
};
pub use node::{bifurcation, biunion};
pub use signal::{Origin, Trackable};

pub mod poll;
pub use sink::GraphSink;
pub use thread::{
    BundlePanics, DefaultThread, ThreadBundle, ThreadBundleHandle, ThreadId, ThreadStream,
    ThreadStreamHandle,
};

#[cfg(test)]
pub mod tests;
