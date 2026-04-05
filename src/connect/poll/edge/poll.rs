//! A same-thread async edge. No Mutex, no Send+Sync overhead.
//!
//! Used for connections between async nodes on the same [crate::thread::AsyncThread].
//! Fires a [Waker] on push to wake the consuming node.
//!
//! For cross-thread connections (sync → async), use [SyncBridge] instead.

use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{Closeable, Pushable, Receivable, closed};
use std::collections::VecDeque;
use std::task::Waker;

/// A same-thread async edge. No synchronization overhead.
///
/// Uses a plain [VecDeque] with no Mutex — all access happens on a single
/// [crate::thread::AsyncThread]. Fires a [Waker] on push to enqueue the
/// consuming node in the ready queue.
///
/// # Safety
///
/// `PollEdge` is marked `Send` so it can be moved from the main thread
/// (during graph construction) to the [crate::thread::AsyncThread]. After
/// the move, all access is single-threaded. This is safe because:
/// - Move semantics prevent the edge from being used on the main thread
///   after it's moved into the async thread
/// - [crate::Pollable::ThreadId] ensures the node (and its edges) can
///   only be added to the correct thread
/// - The [crate::thread::AsyncThread] runs all its nodes on a single OS thread
///
/// `PollEdge` is NOT Sync — it must never be shared across threads.
pub struct PollEdge<DataType, SignalType>
where
    SignalType: Origin,
{
    buffer: VecDeque<Message<DataType, SignalType>>,
    waker: Waker,
    closed: bool,
}

impl<DataType, SignalType> Connection for PollEdge<DataType, SignalType> where SignalType: Origin {}

impl<DataType, SignalType> Pushable for PollEdge<DataType, SignalType>
where
    SignalType: Origin,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, msg: Message<DataType, SignalType>) -> Result<(), Error> {
        PollEdge::push(self, msg)
    }
}

impl<DataType, SignalType> Closeable for PollEdge<DataType, SignalType>
where
    SignalType: Origin,
{
    fn close(&mut self) -> Result<(), Error> {
        PollEdge::close(self)
    }
}

impl<DataType, SignalType> Receivable for PollEdge<DataType, SignalType>
where
    SignalType: Origin,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn try_recv(&mut self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        PollEdge::try_recv(self)
    }
}

impl<DataType, SignalType> PollEdge<DataType, SignalType>
where
    SignalType: Origin,
{
    pub fn new(waker: Waker) -> Self {
        Self {
            buffer: VecDeque::new(),
            waker,
            closed: false,
        }
    }

    /// Non-blocking dequeue.
    pub fn try_recv(&mut self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        match self.buffer.pop_front() {
            Some(msg) => Ok(Some(msg)),
            None if self.closed => Err(closed!()),
            None => Ok(None),
        }
    }

    /// Push a message and wake the consumer.
    pub fn push(&mut self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        if self.closed {
            return Err(closed!());
        }

        self.buffer.push_back(message);
        self.waker.wake_by_ref();
        Ok(())
    }

    /// Close the edge and wake the consumer.
    pub fn close(&mut self) -> Result<(), Error> {
        self.closed = true;
        self.waker.wake_by_ref();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Waker;

    struct TestWaker(Arc<AtomicBool>);

    impl std::task::Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn test_waker() -> (Waker, Arc<AtomicBool>) {
        let woken = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(TestWaker(woken.clone())));
        (waker, woken)
    }

    fn noop_waker() -> Waker {
        std::task::Waker::noop().clone()
    }

    #[test]
    fn push_and_try_recv() {
        let mut edge = PollEdge::<usize, &str>::new(noop_waker());

        edge.push(Message::Data(42)).unwrap();
        assert_eq!(edge.try_recv().unwrap(), Some(Message::Data(42)));
        assert_eq!(edge.try_recv().unwrap(), None);
    }

    #[test]
    fn try_recv_empty_returns_none() {
        let mut edge = PollEdge::<usize, &str>::new(noop_waker());
        assert_eq!(edge.try_recv().unwrap(), None);
    }

    #[test]
    fn closed_try_recv_returns_error() {
        let mut edge = PollEdge::<usize, &str>::new(noop_waker());
        edge.close().unwrap();

        assert!(matches!(
            edge.try_recv().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn closed_push_returns_error() {
        let mut edge = PollEdge::<usize, &str>::new(noop_waker());
        edge.close().unwrap();

        assert!(matches!(
            edge.push(Message::Data(1)).unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn buffered_data_readable_after_close() {
        let mut edge = PollEdge::<usize, &str>::new(noop_waker());
        edge.push(Message::Data(1)).unwrap();
        edge.push(Message::Data(2)).unwrap();

        edge.close().unwrap();

        assert_eq!(edge.try_recv().unwrap(), Some(Message::Data(1)));
        assert_eq!(edge.try_recv().unwrap(), Some(Message::Data(2)));
        assert!(edge.try_recv().is_err());
    }

    #[test]
    fn push_fires_waker() {
        let (waker, woken) = test_waker();
        let mut edge = PollEdge::<usize, &str>::new(waker);

        assert!(!woken.load(Ordering::SeqCst));
        edge.push(Message::Data(1)).unwrap();
        assert!(woken.load(Ordering::SeqCst));
    }

    #[test]
    fn close_fires_waker() {
        let (waker, woken) = test_waker();
        let mut edge = PollEdge::<usize, &str>::new(waker);

        edge.close().unwrap();
        assert!(woken.load(Ordering::SeqCst));
    }
}
