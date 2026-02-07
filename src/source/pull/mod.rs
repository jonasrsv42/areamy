//! Pull-based graph sources.
//!
//! [`Root`] serves as the entry point to a [`Pullable`](crate::Pullable) subgraph.
//! It wraps a [`SyncEdge`](crate::SyncEdge) to receive pushed data and makes it
//! available for pulling by downstream nodes.

use crate::error::Error;
use crate::graph::Get;
use crate::marker::Connection;
use crate::message::Message;
use crate::{Origin, Pullable, Pushable, SyncEdge, ThreadId};
use std::marker::PhantomData;
use std::sync::Arc;

/// [`Root`] of a [`Pullable`] subgraph. It will await input into its [Pushable] and
/// then forward it downstream.
pub struct Root<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    /// The input queue to the [Pullable] chain. For now we have a [SyncEdge]
    /// but will support [std::collections::VecDeque] in the future as well.
    ///
    /// TODO: support having a sync or nosync input.
    pub input: Arc<SyncEdge<DataType, SignalType>>,

    /// The threadId that belongs to the child nodes.
    thread: PhantomData<ThreadIdType>,
}

impl<DataType, SignalType, ThreadIdType> Root<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    pub fn new() -> Self {
        Self {
            input: Arc::new(SyncEdge::new()),
            thread: PhantomData,
        }
    }
}

impl<DataType, SignalType, ThreadIdType> Default for Root<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
    fn default() -> Self {
        Self::new()
    }
}

/// [Root] is a [Pullable] [Connection] in our graph.
impl<DataType, SignalType, ThreadIdType> Connection for Root<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    ThreadIdType: ThreadId,
{
}

/// [Get] the [Pushable] from [Root].
impl<DataType, SignalType, ThreadIdType>
    Get<dyn Pushable<DataType = DataType, SignalType = SignalType>>
    for Root<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = DataType, SignalType = SignalType>>, Error> {
        Get::get(&self.input)
    }
}

/// [Get] the [crate::GraphPushSource] from [Root] for closing.
impl<DataType, SignalType, ThreadIdType>
    Get<dyn crate::GraphPushSource<DataType = DataType, SignalType = SignalType>>
    for Root<DataType, SignalType, ThreadIdType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType: ThreadId,
{
    fn get(
        &self,
    ) -> Result<Box<dyn crate::GraphPushSource<DataType = DataType, SignalType = SignalType>>, Error>
    {
        Get::get(&self.input)
    }
}

/// [Root] is [Pullable] it will serve as the root node of a [Pullable] subgraph.
impl<DataType, SignalType, ThreadIdType> Pullable for Root<DataType, SignalType, ThreadIdType>
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
    use crate::work::Source;

    #[test]
    fn root_can_be_pushed_and_pulled() {
        let mut root = Root::<usize, usize, DefaultThread>::new();
        let mut source = Source::of(&root).unwrap();

        source.push(Message::Data(5)).unwrap();
        source.push(Message::Data(6)).unwrap();

        assert_eq!(root.pull().unwrap(), Message::Data(5));
        assert_eq!(root.pull().unwrap(), Message::Data(6));
    }
}
