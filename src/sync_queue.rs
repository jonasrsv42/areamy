//! A synchronous queue, [std::marker::Sync] compatible connection.
use crate::error::Error;
use crate::{fatal, graph::Get, Pushable};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug)]
/// [`SyncQueue`] is a thread-safe queue with utilites to serve as a graph edge for passing data.
pub struct SyncQueue<T: Clone> {
    buffer: Mutex<VecDeque<T>>,
    signal: Condvar,
}
impl<T: Clone> SyncQueue<T> {
    /// Create empty blocking queue
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            signal: Condvar::new(),
        }
    }
    /// push input on back of queue
    pub fn push_back(&self, t: T) -> Result<(), Error> {
        let mut buffer = self.buffer.lock().map_err(|e| fatal!(e))?;

        buffer.push_back(t);
        self.signal.notify_one();
        Ok(())
    }
    /// read element from front of queue
    pub fn read_front(&self) -> Result<T, Error> {
        let mut buffer = self.buffer.lock().map_err(|e| fatal!(e))?;

        while buffer.len() == 0 {
            buffer = self.signal.wait(buffer).unwrap();
        }

        match buffer.pop_front() {
            Some(element) => Ok(element),
            None => fatal!("non-empty queue with no element. Race condition?").into(),
        }
    }

    /// poll element from front of queue
    pub fn poll(&self) -> Result<Option<T>, Error> {
        let mut buffer = self.buffer.lock().map_err(|e| fatal!(e))?;
        Ok(buffer.pop_front())
    }

    /// wait for element from front of queue
    pub fn wait_front(&self) -> Result<(), Error> {
        let mut buffer = self.buffer.lock().map_err(|e| fatal!(e))?;

        while buffer.len() == 0 {
            buffer = self.signal.wait(buffer).unwrap();
        }

        Ok(())
    }

    /// read element from front of queue
    pub fn push_back_all(&self, items: &Vec<T>) -> Result<(), Error> {
        let mut buffer = self.buffer.lock().map_err(|e| fatal!(e))?;
        for item in items.iter() {
            buffer.push_back(item.clone());
        }
        self.signal.notify_one();
        Ok(())
    }

    /// read element from front of queue
    pub fn read_all(&self) -> Result<Vec<T>, Error> {
        let mut buffer = self.buffer.lock().map_err(|e| fatal!(e))?;

        while buffer.len() == 0 {
            buffer = self.signal.wait(buffer).unwrap();
        }

        let mut all: Vec<T> = Vec::new();
        while buffer.front().is_some() {
            all.push(buffer.pop_front().unwrap());
        }

        Ok(all)
    }
    /// return number of elements in queue
    pub fn len(&self) -> Result<usize, Error> {
        let buffer = self.buffer.lock().map_err(|e| fatal!(e))?;
        Ok(buffer.len())
    }

    /// Check if queue is empty.
    pub fn is_empty(&self) -> Result<bool, Error> {
        let length = self.len()?;
        return Ok(length == 0);
    }
}

impl<T: Clone> crate::marker::Connection for SyncQueue<T> {}

impl<T: Clone + Send + Sync> Pushable for SyncQueue<T> {
    type Message = T;

    fn push(&mut self, object: Self::Message) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<T: Clone + Send + Sync> Pushable for Arc<SyncQueue<T>> {
    type Message = T;

    fn push(&mut self, object: Self::Message) -> Result<(), Error> {
        self.push_back(object)
    }
}

impl<T: Clone + Send + Sync + 'static> Get<dyn Pushable<Message = T>> for Arc<SyncQueue<T>> {
    fn get(&self) -> Result<Box<dyn Pushable<Message = T>>, Error> {
        Ok(Box::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_len() {
        let queue = SyncQueue::<f64>::new();
        assert_eq!(queue.len().unwrap(), 0);
    }
    #[test]
    fn test_push() {
        let queue = SyncQueue::<f64>::new();
        queue.push_back(3.5).unwrap();
        assert_eq!(queue.len().unwrap(), 1);
    }
    #[test]
    fn test_read() {
        let queue = SyncQueue::<f64>::new();
        queue.push_back(3.5).unwrap();
        assert_eq!(queue.read_front().unwrap(), 3.5);
        assert_eq!(queue.len().unwrap(), 0);
    }

    #[test]
    fn test_poll() {
        let queue = SyncQueue::<f64>::new();
        assert_eq!(queue.poll().unwrap(), None);
        queue.push_back(3.5).unwrap();
        assert_eq!(queue.poll().unwrap(), Some(3.5));
        assert_eq!(queue.poll().unwrap(), None);
    }

    #[test]
    fn test_read_all() {
        let queue = SyncQueue::<f64>::new();
        queue.push_back(3.5).unwrap();
        queue.push_back(4.0).unwrap();

        let all = queue.read_all().unwrap();
        assert_eq!(all[0], 3.5);
        assert_eq!(all[1], 4.0);
    }
}
