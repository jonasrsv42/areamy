use crate::graph::{Closeable, TryPushable};

/// The common base of every output: something you can [TryPushable::try_push] into without
/// blocking, and [Closeable::close]. [crate::work::Sink] adds blocking push on top.
pub trait Sink: TryPushable + Closeable {}
impl<T: TryPushable + Closeable> Sink for T {}
