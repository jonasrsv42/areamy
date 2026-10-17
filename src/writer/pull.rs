//! Pull-based graph writers.
//!
//! This module provides entry points for pull-based graph segments.
//!
//! # Writer Types
//!
//! There are two ways to feed data into a pull graph:
//!
//! - **[`WriterBuffer`]**: A buffer you push data into, which can then be pulled by downstream
//!   nodes. Use this when you have in-memory data or want to bridge from a push-based source.
//!
//! - **[`PullWriter`](crate::pull::PullWriter)**: A trait for "true" pull sources that
//!   implement [`Pullable`] directly, like file readers. These pull data
//!   on-demand rather than buffering.

use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::Get;
use crate::graph::marker::Connection;
use crate::message::Message;
use crate::pull::Pullable;
use crate::signal::Origin;
use crate::thread::ThreadId;
use core::marker::PhantomData;
use core::num::NonZeroUsize;

/// A buffer that serves as the entry point to a [`Pullable`] subgraph.
///
/// [`WriterBuffer`] accepts pushed data via [`Pushable`](crate::graph::Pushable) and makes it available for pulling
/// by downstream nodes. This is useful for bridging push-based data sources into pull-based
/// graph segments.
///
/// For "true" pull sources that read data on-demand (like file readers), implement
/// [`PullWriter`](crate::pull::PullWriter) directly instead.
///
/// # Example
///
/// ```
/// use areamy::{Message, Pullable, Pushable, DefaultThread};
/// use areamy::pull::WriterBuffer;
/// use areamy::work::Writer;
///
/// let mut buffer = WriterBuffer::<usize, usize, DefaultThread>::new();
/// let mut writer = Writer::of(&buffer).unwrap();
///
/// // Push data into the buffer
/// writer.push(Message::Data(1)).unwrap();
/// writer.push(Message::Data(2)).unwrap();
///
/// // Pull data out
/// assert_eq!(buffer.pull().unwrap(), Message::Data(1));
/// assert_eq!(buffer.pull().unwrap(), Message::Data(2));
/// ```
pub struct WriterBuffer<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    /// The input queue to the [Pullable] chain.
    pub input: Receiver<DataType, SignalType>,

    /// The threadId of the downstream chain.
    thread: PhantomData<ThreadIdType>,
}

impl<DataType, SignalType, ThreadIdType> WriterBuffer<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    pub fn new() -> Self {
        Self {
            input: Receiver::new(),
            thread: PhantomData,
        }
    }

    /// Like [`WriterBuffer::new`], but holding at most `bound` messages, signals included; a
    /// writer waits while it is full. Write from another thread than the one pulling: a full
    /// buffer only drains when pulled.
    pub fn bounded(bound: NonZeroUsize) -> Self {
        Self {
            input: Receiver::bounded(bound),
            thread: PhantomData,
        }
    }
}

impl<DataType, SignalType, ThreadIdType> Default
    for WriterBuffer<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    fn default() -> Self {
        Self::new()
    }
}

/// [WriterBuffer] is a [Pullable] [Connection] in our graph.
impl<DataType, SignalType, ThreadIdType> Connection
    for WriterBuffer<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
}

impl<ConnectionType, DataType, SignalType, ThreadIdType> Get<ConnectionType>
    for WriterBuffer<DataType, SignalType, ThreadIdType>
where
    ConnectionType: Connection + ?Sized,
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
    Receiver<DataType, SignalType>: Get<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Get::get(&self.input)
    }
}

/// [WriterBuffer] is [Pullable] - it serves as the root node of a [Pullable] subgraph.
impl<DataType, SignalType, ThreadIdType> Pullable
    for WriterBuffer<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    type ThreadId = ThreadIdType;
    type DataType = DataType;
    type SignalType = SignalType;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        self.input.read_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultThread;
    use crate::graph::{Pushable, TryPush, TryPushable};
    use crate::work::Writer;

    #[test]
    fn writer_buffer_can_be_pushed_and_pulled() {
        let mut buffer = WriterBuffer::<usize, usize, DefaultThread>::new();
        let mut writer = Writer::of(&buffer).unwrap();

        writer.push(Message::Data(5)).unwrap();
        writer.push(Message::Data(6)).unwrap();

        assert_eq!(buffer.pull().unwrap(), Message::Data(5));
        assert_eq!(buffer.pull().unwrap(), Message::Data(6));
    }

    #[test]
    fn a_bounded_writer_buffer_refuses_until_pulled() {
        let mut buffer = WriterBuffer::<usize, usize, DefaultThread>::bounded(NonZeroUsize::MIN);
        let mut writer = Writer::of(&buffer).unwrap();

        assert_eq!(writer.try_push(Message::Data(5)).unwrap(), TryPush::Pushed);
        assert!(matches!(
            writer.try_push(Message::Data(6)).unwrap(),
            TryPush::Full(_)
        ));
        assert_eq!(buffer.pull().unwrap(), Message::Data(5));
        assert_eq!(writer.try_push(Message::Data(6)).unwrap(), TryPush::Pushed);
    }
}
