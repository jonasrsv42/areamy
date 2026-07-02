extern crate alloc;

mod combine;
pub mod composable;
pub mod connect;
pub mod error;
pub mod message;
pub mod node;
pub mod reader;
mod signal;
pub mod thread;
pub mod work;
pub mod writer;
pub use writer::writer::PullWriter;
mod contains;
mod generates;

pub mod pull;

pub use combine::Combine;
pub use composable::{Composable, Decomposable};
pub use connect::marker;
pub use contains::Contains;
pub use generates::Generates;

pub use crate::connect::graph;
pub use connect::sync;
pub use connect::{
    Closeable, PolicyEdge, Pollable, Pullable, Pushable, Receivable, SignalPolicy, Sink, Workable,
    make_bidi, make_push, make_work, marker::Connection,
};
pub use message::Message;
pub use node::{
    BifurcationIo, BifurcationRoutine, BiunionIo, BiunionRoutine, Flush, LineIo, LineRoutine, Next,
    Poll, Send,
};
pub use node::{bifurcation, biunion};
pub use signal::{Origin, Trackable};

pub mod poll;
pub use reader::Reader;
pub use thread::{
    DefaultThread, ThreadBundle, ThreadBundleHandle, ThreadId, ThreadStream, ThreadStreamHandle,
};

#[cfg(test)]
pub mod tests;
