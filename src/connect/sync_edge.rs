//! A synchronous edge, [std::marker::Sync] compatible connection for Message passing.
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{fatal, graph::Get, Pushable};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug)]
/// Tracks the internal state of SyncEdge
struct Inner<DataType, SignalType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    buffer: VecDeque<Message<DataType, SignalType>>,
}

#[derive(Debug)]
/// [`SyncEdge`] is a thread-safe queue for passing Messages between graph nodes.
pub struct SyncEdge<DataType, SignalType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    inner: Mutex<Inner<DataType, SignalType>>,
    signal: Condvar,
}

impl<DataType, SignalType> SyncEdge<DataType, SignalType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    /// Create empty blocking queue
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
            }),
            signal: Condvar::new(),
        }
    }
    
    /// push input on back of queue
    pub fn push_back(&self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;
        
        inner.buffer.push_back(message);
        self.signal.notify_one();
        
        Ok(())
    }
    
    /// read element from front of queue
    pub fn read_front(&self) -> Result<Message<DataType, SignalType>, Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        while inner.buffer.len() == 0 {
            inner = self.signal.wait(inner).unwrap();
        }

        match inner.buffer.pop_front() {
            Some(element) => Ok(element),
            None => fatal!("non-empty queue with no element. Race condition?").into(),
        }
    }

    /// poll element from front of queue
    pub fn poll(&self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;
        Ok(inner.buffer.pop_front())
    }

    /// wait for element from front of queue
    pub fn wait_front(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        while inner.buffer.len() == 0 {
            inner = self.signal.wait(inner).unwrap();
        }

        Ok(())
    }

    /// push multiple messages to the back of queue
    pub fn push_back_all(&self, items: &Vec<Message<DataType, SignalType>>) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;
        
        for item in items.iter() {
            inner.buffer.push_back(item.clone());
        }
        
        self.signal.notify_one();
        
        Ok(())
    }

    /// read all elements from the queue
    pub fn read_all(&self) -> Result<Vec<Message<DataType, SignalType>>, Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        while inner.buffer.len() == 0 {
            inner = self.signal.wait(inner).unwrap();
        }

        let mut all: Vec<Message<DataType, SignalType>> = Vec::new();
        while inner.buffer.front().is_some() {
            all.push(inner.buffer.pop_front().unwrap());
        }

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
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{}

impl<DataType, SignalType> Pushable for SyncEdge<DataType, SignalType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<DataType, SignalType> Pushable for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<DataType, SignalType> Get<dyn Pushable<DataType = DataType, SignalType = SignalType>> for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = DataType, SignalType = SignalType>>, Error> {
        Ok(Box::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Origin;
    
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    
    // We're now using PolicyEdge wrapper instead of SyncEdge with policy
// These tests moved to signal_policy.rs
}
