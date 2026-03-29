//! A thread-safe bridge from sync nodes to async nodes.
//!
//! [SyncBridge] is used when a sync node (on a [crate::thread::ThreadStream])
//! pushes data into an async node (on a [crate::thread::AsyncThread]).
//! It uses a [Mutex] for thread-safety and fires a [Waker] on push.
//!
//! For same-thread async→async connections, use [super::edge::AsyncEdge] instead.

use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{closed, fatal, graph::Get};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::Waker;

struct Inner<DataType, SignalType: Origin> {
    buffer: VecDeque<Message<DataType, SignalType>>,
    waker: Waker,
    closed: bool,
}

/// Thread-safe bridge from sync to async nodes.
///
/// Implements [crate::Closeable] so sync nodes can push into it via
/// [crate::make_push]. Fires a [Waker] on every push to wake the
/// consuming async node.
pub struct SyncBridge<DataType, SignalType: Origin> {
    inner: Mutex<Inner<DataType, SignalType>>,
}

impl<DataType, SignalType> SyncBridge<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    pub fn new(waker: Waker) -> Self {
        Self {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
                waker,
                closed: false,
            }),
        }
    }

    pub fn try_recv(&self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        match inner.buffer.pop_front() {
            Some(msg) => Ok(Some(msg)),
            None if inner.closed => Err(closed!()),
            None => Ok(None),
        }
    }

    pub fn push_back(&self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;

        if inner.closed {
            return Err(closed!());
        }

        inner.buffer.push_back(message);
        inner.waker.wake_by_ref();

        Ok(())
    }

    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        inner.waker.wake_by_ref();

        Ok(())
    }
}

impl<DataType, SignalType> Connection for SyncBridge<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
}

impl<DataType, SignalType> crate::Receivable for Arc<SyncBridge<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn try_recv(&mut self) -> Result<Option<Message<DataType, SignalType>>, crate::error::Error> {
        SyncBridge::try_recv(self)
    }
}

impl<DataType, SignalType> crate::Pushable for Arc<SyncBridge<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.push_back(msg)
    }
}

impl<DataType, SignalType> crate::Closeable for Arc<SyncBridge<DataType, SignalType>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn close(&mut self) -> Result<(), Error> {
        SyncBridge::close(self)
    }
}

impl<DataType, SignalType>
    Get<dyn crate::Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync>
    for Arc<SyncBridge<DataType, SignalType>>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn crate::Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync>,
        Error,
    > {
        Ok(Box::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Origin;
    use crate::error::ErrorKind;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestSignal;
    impl Origin for TestSignal {}

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
        let bridge = SyncBridge::<usize, TestSignal>::new(noop_waker());

        bridge.push_back(Message::Data(42)).unwrap();
        assert_eq!(bridge.try_recv().unwrap(), Some(Message::Data(42)));
        assert_eq!(bridge.try_recv().unwrap(), None);
    }

    #[test]
    fn try_recv_empty_returns_none() {
        let bridge = SyncBridge::<usize, TestSignal>::new(noop_waker());
        assert_eq!(bridge.try_recv().unwrap(), None);
    }

    #[test]
    fn closed_try_recv_returns_error() {
        let bridge = SyncBridge::<usize, TestSignal>::new(noop_waker());
        bridge.close().unwrap();

        assert!(matches!(
            bridge.try_recv().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn closed_push_returns_error() {
        let bridge = SyncBridge::<usize, TestSignal>::new(noop_waker());
        bridge.close().unwrap();

        assert!(matches!(
            bridge.push_back(Message::Data(1)).unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn buffered_data_readable_after_close() {
        let bridge = SyncBridge::<usize, TestSignal>::new(noop_waker());
        bridge.push_back(Message::Data(1)).unwrap();
        bridge.push_back(Message::Data(2)).unwrap();

        bridge.close().unwrap();

        assert_eq!(bridge.try_recv().unwrap(), Some(Message::Data(1)));
        assert_eq!(bridge.try_recv().unwrap(), Some(Message::Data(2)));
        assert!(bridge.try_recv().is_err());
    }

    #[test]
    fn push_fires_waker() {
        let (waker, woken) = test_waker();
        let bridge = SyncBridge::<usize, TestSignal>::new(waker);

        assert!(!woken.load(Ordering::SeqCst));
        bridge.push_back(Message::Data(1)).unwrap();
        assert!(woken.load(Ordering::SeqCst));
    }

    #[test]
    fn close_fires_waker() {
        let (waker, woken) = test_waker();
        let bridge = SyncBridge::<usize, TestSignal>::new(waker);

        bridge.close().unwrap();
        assert!(woken.load(Ordering::SeqCst));
    }

    #[test]
    fn closeable_via_arc() {
        use crate::Closeable;

        let bridge = Arc::new(SyncBridge::<usize, TestSignal>::new(noop_waker()));
        let mut closeable: Box<dyn Closeable<DataType = usize, SignalType = TestSignal>> =
            Box::new(bridge.clone());

        closeable.push(Message::Data(99)).unwrap();
        assert_eq!(bridge.try_recv().unwrap(), Some(Message::Data(99)));

        closeable.close().unwrap();
        assert!(bridge.try_recv().is_err());
    }
}
