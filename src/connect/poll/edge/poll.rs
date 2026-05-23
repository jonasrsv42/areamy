//! A same-thread async edge.
//!
//! Used for connections between async nodes on the same [crate::thread::poll::stream::Thread].
//! Fires a [Waker] on push to wake the consuming node.
//!
//! Unlike sync variants that need separate producer and consumer for clean
//! teardown semantics this is a single shared queue, this is sound because
//! this will only-ever be owned by a single thread and so its fully cleaned up
//! one teardown of that thread.

use crate::connect::waker::ThreadLocalWaker;
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{Closeable, Pushable, Receivable, closed};
use std::collections::VecDeque;

/// A same-thread async edge. No synchronization overhead.
///
/// Uses a plain [VecDeque] with no Mutex — all access happens on a single
/// [crate::thread::poll::stream::Thread]. Fires a [ThreadLocalWaker] on push to
/// enqueue the consuming node in the ready queue.
///
/// `PollEdge` is `!Send`, `!Sync` — it must stay on the async thread.
pub struct PollEdge<DataType, SignalType>
where
    SignalType: Origin,
{
    buffer: VecDeque<Message<DataType, SignalType>>,
    waker: ThreadLocalWaker,
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
    pub fn new(waker: ThreadLocalWaker) -> Self {
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
        self.waker.wake();
        Ok(())
    }

    /// Close the edge and wake the consumer.
    pub fn close(&mut self) -> Result<(), Error> {
        self.closed = true;
        self.waker.wake();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::waker::ThreadLocalWake;
    use crate::error::ErrorKind;
    use std::cell::Cell;
    use std::rc::Rc;

    struct TestWake(Rc<Cell<bool>>);

    impl ThreadLocalWake for TestWake {
        fn wake(&self) {
            self.0.set(true);
        }
    }

    fn test_waker() -> (ThreadLocalWaker, Rc<Cell<bool>>) {
        let woken = Rc::new(Cell::new(false));
        let waker = ThreadLocalWaker::new(TestWake(woken.clone()));
        (waker, woken)
    }

    fn noop_waker() -> ThreadLocalWaker {
        struct NoopWake;
        impl ThreadLocalWake for NoopWake {
            fn wake(&self) {}
        }
        ThreadLocalWaker::new(NoopWake)
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

        assert!(!woken.get());
        edge.push(Message::Data(1)).unwrap();
        assert!(woken.get());
    }

    #[test]
    fn close_fires_waker() {
        let (waker, woken) = test_waker();
        let mut edge = PollEdge::<usize, &str>::new(waker);

        edge.close().unwrap();
        assert!(woken.get());
    }
}
