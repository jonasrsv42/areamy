//! A synchronous edge, [std::marker::Sync] compatible connection for Message passing.
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{Pushable, closed, fatal, graph::Get};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug)]
/// Tracks the internal state of SyncEdge
struct Inner<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    buffer: VecDeque<Message<DataType, SignalType>>,
    closed: bool,
}

#[derive(Debug)]
/// [`SyncEdge`] is a thread-safe queue for passing Messages between graph nodes.
pub struct SyncEdge<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    inner: Mutex<Inner<DataType, SignalType>>,
    signal: Condvar,
}

impl<DataType, SignalType> SyncEdge<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    /// Create empty blocking queue
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
                closed: false,
            }),
            signal: Condvar::new(),
        }
    }

    /// Close this edge. After closing:
    /// - Reads will return remaining buffered data
    /// - When buffer is empty, reads return [ErrorKind::Closed]
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        self.signal.notify_all();
        Ok(())
    }

    /// Push input on back of queue.
    /// Returns [ErrorKind::Closed] if the edge is closed.
    pub fn push_back(&self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        if inner.closed {
            return Err(closed!());
        }

        inner.buffer.push_back(message);
        self.signal.notify_one();

        Ok(())
    }

    /// Read element from front of queue. Blocks until data is available.
    /// Returns [ErrorKind::Closed] if closed and buffer is empty.
    pub fn read_front(&self) -> Result<Message<DataType, SignalType>, Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        while inner.buffer.is_empty() {
            if inner.closed {
                return Err(closed!());
            }
            inner = self.signal.wait(inner).map_err(|e| fatal!(e))?;
        }

        match inner.buffer.pop_front() {
            Some(element) => Ok(element),
            None => fatal!("non-empty queue with no element").into(),
        }
    }

    /// Poll element from front of queue (non-blocking).
    /// Returns [ErrorKind::Closed] if closed and buffer is empty.
    /// Returns Ok(None) if open but buffer is empty.
    /// Returns Ok(Some(msg)) if there's data.
    pub fn poll(&self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        match inner.buffer.pop_front() {
            Some(msg) => Ok(Some(msg)),
            None if inner.closed => Err(closed!()),
            None => Ok(None),
        }
    }

    /// Wait for element from front of queue. Blocks until data is available.
    /// Returns [ErrorKind::Closed] if closed and buffer is empty.
    pub fn wait_front(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        while inner.buffer.is_empty() {
            if inner.closed {
                return Err(closed!());
            }
            inner = self.signal.wait(inner).map_err(|e| fatal!(e))?;
        }

        Ok(())
    }

    /// Push multiple messages to the back of queue.
    /// Returns [ErrorKind::Closed] if the edge is closed.
    pub fn push_back_all(&self, items: Vec<Message<DataType, SignalType>>) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        if inner.closed {
            return Err(closed!());
        }

        for item in items.into_iter() {
            inner.buffer.push_back(item);
        }

        self.signal.notify_one();

        Ok(())
    }

    /// Read all elements from the queue. Blocks until at least one is available.
    /// Returns [ErrorKind::Closed] if closed and buffer is empty.
    pub fn read_all(&self) -> Result<Vec<Message<DataType, SignalType>>, Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        while inner.buffer.is_empty() {
            if inner.closed {
                return Err(closed!());
            }
            inner = self.signal.wait(inner).map_err(|e| fatal!(e))?;
        }

        let all: Vec<Message<DataType, SignalType>> = inner.buffer.drain(..).collect();

        Ok(all)
    }

    /// return number of elements in queue
    pub fn len(&self) -> Result<usize, Error> {
        let inner = self.inner.lock().map_err(|e| fatal!(e))?;
        Ok(inner.buffer.len())
    }

    /// Check if queue is empty.
    pub fn is_empty(&self) -> Result<bool, Error> {
        let length = self.len()?;
        return Ok(length == 0);
    }
}

impl<DataType, SignalType> Connection for SyncEdge<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
}

impl<DataType, SignalType> Pushable for SyncEdge<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<DataType, SignalType> Pushable for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<DataType, SignalType> crate::GraphPushSource for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn close(&mut self) -> Result<(), Error> {
        SyncEdge::close(self)
    }
}

impl<DataType, SignalType> Get<dyn Pushable<DataType = DataType, SignalType = SignalType>>
    for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = DataType, SignalType = SignalType>>, Error> {
        Ok(Box::new(self.clone()))
    }
}

impl<DataType, SignalType>
    Get<dyn crate::GraphPushSource<DataType = DataType, SignalType = SignalType>>
    for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn crate::GraphPushSource<DataType = DataType, SignalType = SignalType>>, Error>
    {
        Ok(Box::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Origin;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestSignal(u32);

    impl Origin for TestSignal {}

    #[test]
    fn test_len() {
        let edge = SyncEdge::<f64, TestSignal>::new();
        assert_eq!(edge.len().unwrap(), 0);
    }

    #[test]
    fn test_push() {
        let edge = SyncEdge::<f64, TestSignal>::new();
        edge.push_back(Message::Data(3.5)).unwrap();
        assert_eq!(edge.len().unwrap(), 1);
    }

    #[test]
    fn test_read() {
        let edge = SyncEdge::<f64, TestSignal>::new();
        edge.push_back(Message::Data(3.5)).unwrap();
        assert_eq!(edge.read_front().unwrap(), Message::Data(3.5));
        assert_eq!(edge.len().unwrap(), 0);
    }

    #[test]
    fn test_poll() {
        let edge = SyncEdge::<f64, TestSignal>::new();
        assert_eq!(edge.poll().unwrap(), None);
        edge.push_back(Message::Data(3.5)).unwrap();
        assert_eq!(edge.poll().unwrap(), Some(Message::Data(3.5)));
        assert_eq!(edge.poll().unwrap(), None);
    }

    #[test]
    fn test_read_all() {
        let edge = SyncEdge::<f64, TestSignal>::new();
        edge.push_back(Message::Data(3.5)).unwrap();
        edge.push_back(Message::Data(4.0)).unwrap();

        let all = edge.read_all().unwrap();
        assert_eq!(all[0], Message::Data(3.5));
        assert_eq!(all[1], Message::Data(4.0));
    }

    #[test]
    fn test_signal_messages() {
        let edge = SyncEdge::<f64, TestSignal>::new();
        edge.push_back(Message::Flush(TestSignal(1))).unwrap();
        edge.push_back(Message::Marker(TestSignal(2))).unwrap();

        let messages = edge.read_all().unwrap();
        assert_eq!(messages[0], Message::Flush(TestSignal(1)));
        assert_eq!(messages[1], Message::Marker(TestSignal(2)));
    }

    #[test]
    fn test_close_can_read_buffered_data() {
        let edge = SyncEdge::<f64, TestSignal>::new();
        edge.push_back(Message::Data(1.0)).unwrap();
        edge.push_back(Message::Data(2.0)).unwrap();

        edge.close().unwrap();

        // Can still read buffered data after close
        assert_eq!(edge.read_front().unwrap(), Message::Data(1.0));
        assert_eq!(edge.read_front().unwrap(), Message::Data(2.0));
    }

    #[test]
    fn test_close_poll_returns_closed_when_empty() {
        use crate::error::ErrorKind;

        let edge = SyncEdge::<f64, TestSignal>::new();
        edge.push_back(Message::Data(1.0)).unwrap();

        edge.close().unwrap();

        // Can still poll buffered data
        assert_eq!(edge.poll().unwrap(), Some(Message::Data(1.0)));

        // Now buffer is empty - poll returns Closed error
        let result = edge.poll();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn test_close_errors_on_push() {
        use crate::error::ErrorKind;

        let edge = SyncEdge::<f64, TestSignal>::new();
        edge.close().unwrap();

        // Push after close should error
        let result = edge.push_back(Message::Data(1.0));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn test_poll_returns_none_when_open_and_empty() {
        let edge = SyncEdge::<f64, TestSignal>::new();

        // poll on open empty edge returns None
        assert_eq!(edge.poll().unwrap(), None);
    }

    // We're now using PolicyEdge wrapper instead of SyncEdge with policy
    // These tests moved to signal_policy.rs
}
