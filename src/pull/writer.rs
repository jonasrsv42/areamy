use crate::graph::Closeable;
use crate::pull::Pullable;

/// A [PullWriter] is a data source that can be pulled from, typically implementing a Read
/// interface. It is [Pullable] (consumed by pullable components) and [Closeable] (signals no
/// more data). A prototypical usecase is a File input reader.
///
/// See [crate::writer] for details on shutdown flow.
pub trait PullWriter: Pullable + Closeable {}
impl<T: Pullable + Closeable> PullWriter for T {}
