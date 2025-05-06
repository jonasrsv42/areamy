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
pub use source::source::Source;

pub mod pull;

pub use composable::Composable;
pub use connect::marker;

pub use crate::connect::graph;
pub use connect::{
    make_bidi, make_push, make_work, marker::Connection, Pullable, PolicyEdge, Pushable, SignalPolicy, SyncEdge, Workable,
};
pub use message::Message;
pub use node::{bifurcation, biunion};
pub use node::{
    BifurcationReader, BifurcationRoutine, BiunionReader, BiunionRoutine, Flush, LineReader,
    LineRoutine, Next, Send,
};
pub use signal::{Origin, Trackable};
pub use sink::Sink;
pub use thread::thread_id::{DefaultThread, ThreadId};
pub use thread::thread_stream::ThreadStream;

#[cfg(test)]
pub mod tests;
