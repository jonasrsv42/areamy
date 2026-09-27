// Graph types are deep by nature.
#![allow(clippy::type_complexity)]

extern crate alloc;

mod combine;
pub mod composable;
pub mod edge;
pub mod error;
pub mod graph;
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
pub use contains::Contains;
pub use generates::Generates;

pub use edge::policy::{PolicyEdge, SignalPolicy};
pub use edge::push::make_push;
pub use edge::sync;
pub use graph::marker::{self, Connection};
pub use graph::{Closeable, Pushable, Receivable, Sink};
pub use message::Message;
pub use node::{
    BifurcationIo, BifurcationRoutine, BiunionIo, BiunionRoutine, Flush, LineIo, LineRoutine, Next,
    Poll, Send,
};
pub use node::{bifurcation, biunion};
pub use poll::Pollable;
pub use pull::Pullable;
pub use signal::{Origin, Trackable};
pub use work::{Workable, make_bidi, make_work};

pub mod poll;
pub use reader::Reader;
pub use thread::{
    DefaultThread, ThreadBundle, ThreadBundleHandle, ThreadId, ThreadStream, ThreadStreamHandle,
};

#[cfg(test)]
pub mod tests;
