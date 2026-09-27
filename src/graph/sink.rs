use crate::graph::{Closeable, Pushable};

/// A [`Sink`] is the fundamental data output: something you can [Pushable::push] into
/// and [Closeable::close]. Nodes hold their outputs as [`Sink`]s and real terminal sinks
/// inherit it.
pub trait Sink: Pushable + Closeable {}
impl<T: Pushable + Closeable> Sink for T {}
