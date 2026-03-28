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
    AsyncEdge, Closeable, Linkable, PolicyEdge, Pollable, Pullable, Pushable, Receivable,
    SignalPolicy, SyncBridge, SyncEdge, Workable, make_bidi, make_push, make_work,
    marker::Connection,
};
pub use message::Message;
pub use node::{
    AsyncLineRoutine, BifurcationReader, BifurcationRoutine, BiunionReader, BiunionRoutine, Flush,
    LineReader, LineRoutine, Next, Poll, Send,
};
pub use node::{bifurcation, biunion};
pub use signal::{Origin, Trackable};
pub use sink::GraphSink;
pub use thread::{
    AsyncThread, AsyncThreadHandle, BundlePanics, DefaultThread, Spawnable, ThreadBundle,
    ThreadBundleHandle, ThreadId, ThreadStream, ThreadStreamHandle,
};

#[cfg(test)]
pub mod tests;
