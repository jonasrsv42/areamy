//! A synchronous edge, [std::marker::Sync] compatible connection for Message passing.
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{fatal, graph::Get, Pushable};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug, Clone, Copy, PartialEq)]
/// Policy for handling signals in the queue
pub enum SignalPolicy {
    /// Always forward signals into the queue
    Forward,
    /// Only forward signals if the last entry was not a signal
    ForwardAfterData,
    /// Never forward signals into the queue
    Block,
}

#[derive(Debug)]
/// Tracks the internal state of SyncEdge
struct Inner<DataType, SignalType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    buffer: VecDeque<Message<DataType, SignalType>>,
    last_was_data: bool,
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
    policy: SignalPolicy,
}

impl<DataType, SignalType> SyncEdge<DataType, SignalType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    /// Create empty blocking queue with default Forward policy
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
                last_was_data: false,
            }),
            signal: Condvar::new(),
            policy: SignalPolicy::Forward,
        }
    }
    
    /// Create a new SyncEdge with the specified signal policy
    pub fn with_policy(policy: SignalPolicy) -> Self {
        Self {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
                last_was_data: false,
            }),
            signal: Condvar::new(),
            policy,
        }
    }
    
    /// Helper method to handle message addition based on policy
    fn add_message(&self, inner: &mut Inner<DataType, SignalType>, message: Message<DataType, SignalType>) -> bool {
        // Check if the message is a signal
        let is_signal = match message {
            Message::Data(_) => false,
            Message::Flush(_) | Message::Marker(_) => true,
        };

        // Apply policy for signals
        if is_signal {
            match self.policy {
                SignalPolicy::Forward => {
                    // Always forward signals
                    inner.buffer.push_back(message);
                    inner.last_was_data = false;
                    true
                },
                SignalPolicy::ForwardAfterData => {
                    // Only forward if the last entry was data
                    if inner.last_was_data {
                        inner.buffer.push_back(message);
                        inner.last_was_data = false;
                        true
                    } else {
                        // Skip this signal
                        false
                    }
                },
                SignalPolicy::Block => {
                    // Never forward signals
                    false
                }
            }
        } else {
            // Always push data messages
            inner.buffer.push_back(message);
            inner.last_was_data = true;
            true
        }
    }
    
    /// push input on back of queue
    pub fn push_back(&self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;
        
        let added = self.add_message(&mut inner, message);
        
        if added {
            self.signal.notify_one();
        }
        
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
        let mut added = false;
        
        for item in items.iter() {
            if self.add_message(&mut inner, item.clone()) {
                added = true;
            }
        }
        
        if added {
            self.signal.notify_one();
        }
        
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
    type Message = Message<DataType, SignalType>;

    fn push(&mut self, object: Self::Message) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<DataType, SignalType> Pushable for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    type Message = Message<DataType, SignalType>;

    fn push(&mut self, object: Self::Message) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<DataType, SignalType> Get<dyn Pushable<Message = Message<DataType, SignalType>>> for Arc<SyncEdge<DataType, SignalType>>
where
    DataType: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Pushable<Message = Message<DataType, SignalType>>>, Error> {
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
    
    #[test]
    fn test_signal_policy_forward() {
        let edge = SyncEdge::<f64, TestSignal>::with_policy(SignalPolicy::Forward);
        
        // Forward policy should allow all signals
        edge.push_back(Message::Flush(TestSignal(1))).unwrap();
        edge.push_back(Message::Marker(TestSignal(2))).unwrap();
        edge.push_back(Message::Flush(TestSignal(3))).unwrap();
        
        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0], Message::Flush(TestSignal(1)));
        assert_eq!(messages[1], Message::Marker(TestSignal(2)));
        assert_eq!(messages[2], Message::Flush(TestSignal(3)));
    }
    
    #[test]
    fn test_signal_policy_forward_after_data() {
        let edge = SyncEdge::<f64, TestSignal>::with_policy(SignalPolicy::ForwardAfterData);
        
        // First signal should be dropped (no data yet)
        edge.push_back(Message::Flush(TestSignal(1))).unwrap();
        assert_eq!(edge.len().unwrap(), 0);
        
        // Add data
        edge.push_back(Message::Data(3.5)).unwrap();
        assert_eq!(edge.len().unwrap(), 1);
        
        // Now signal should be forwarded
        edge.push_back(Message::Marker(TestSignal(2))).unwrap();
        assert_eq!(edge.len().unwrap(), 2);
        
        // This signal should be dropped (previous was a signal)
        edge.push_back(Message::Flush(TestSignal(3))).unwrap();
        assert_eq!(edge.len().unwrap(), 2);
        
        // Add more data
        edge.push_back(Message::Data(4.0)).unwrap();
        
        // Now signal should be forwarded again
        edge.push_back(Message::Flush(TestSignal(4))).unwrap();
        
        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0], Message::Data(3.5));
        assert_eq!(messages[1], Message::Marker(TestSignal(2)));
        assert_eq!(messages[2], Message::Data(4.0));
        assert_eq!(messages[3], Message::Flush(TestSignal(4)));
    }
    
    #[test]
    fn test_signal_policy_block() {
        let edge = SyncEdge::<f64, TestSignal>::with_policy(SignalPolicy::Block);
        
        // All signals should be blocked
        edge.push_back(Message::Flush(TestSignal(1))).unwrap();
        edge.push_back(Message::Marker(TestSignal(2))).unwrap();
        assert_eq!(edge.len().unwrap(), 0);
        
        // Data should still go through
        edge.push_back(Message::Data(3.5)).unwrap();
        edge.push_back(Message::Data(4.0)).unwrap();
        assert_eq!(edge.len().unwrap(), 2);
        
        // More signals should be blocked
        edge.push_back(Message::Flush(TestSignal(3))).unwrap();
        assert_eq!(edge.len().unwrap(), 2);
        
        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], Message::Data(3.5));
        assert_eq!(messages[1], Message::Data(4.0));
    }
    
    #[test]
    fn test_push_back_all_with_policy() {
        let edge = SyncEdge::<f64, TestSignal>::with_policy(SignalPolicy::ForwardAfterData);
        
        let messages = vec![
            Message::Flush(TestSignal(1)),     // Should be dropped (no data before)
            Message::Data(1.0),                // Should be forwarded
            Message::Marker(TestSignal(2)),    // Should be forwarded (after data)
            Message::Flush(TestSignal(3)),     // Should be dropped (after signal)
            Message::Data(2.0),                // Should be forwarded
            Message::Flush(TestSignal(4)),     // Should be forwarded (after data)
        ];
        
        edge.push_back_all(&messages).unwrap();
        
        let result = edge.read_all().unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Message::Data(1.0));
        assert_eq!(result[1], Message::Marker(TestSignal(2)));
        assert_eq!(result[2], Message::Data(2.0));
        assert_eq!(result[3], Message::Flush(TestSignal(4)));
    }
}