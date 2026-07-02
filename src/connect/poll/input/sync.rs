//! Thread-safe bridge from sync producers to a poll consumer.
//!
//! [`Sender`] is `Send + Sync + Clone`, held by sync threads pushing
//! data. [`Receiver`] is the poll node's read end, holding the
//! pre-allocated [`Waker`] that gets fired on every push or close.
//!
//! Refcounted close-on-drop: when the last [`Sender`] is dropped, the
//! edge auto-closes and the consumer waker fires, so the poll node
//! observes `Closed` on its next poll. When the [`Receiver`] is
//! dropped, further pushes return `Closed`.

use crate::connect::poll::wakers::allocator::Slot;
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{Closeable, Pushable, Receivable, Sink, closed, fatal, graph::Get};
use std::cell::Cell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;

/// Sync input edge paired with its pre-allocated waker slot.
/// Created during `.input::<Sync>()` on the main thread.
pub struct Input<DataType, SignalType: Origin> {
    pub edge: Receiver<DataType, SignalType>,
    pub slot: Slot<Waker>,
}

struct Inner<DataType, SignalType: Origin> {
    buffer: VecDeque<Message<DataType, SignalType>>,
    waker: Waker,
    closed: bool,
}

struct Shared<DataType, SignalType: Origin> {
    inner: Mutex<Inner<DataType, SignalType>>,
    producer_count: AtomicUsize,
}

/// Producer handle. `Send + Sync + Clone` when `DataType` and
/// `SignalType` are `Send + Sync`. Dropping the last clone closes the
/// edge and fires the consumer waker.
pub struct Sender<DataType, SignalType: Origin> {
    shared: Arc<Shared<DataType, SignalType>>,
}

/// Single-consumer handle held by the poll node. `Send + !Sync + !Clone`
/// when `DataType` and `SignalType` are `Send`.
pub struct Receiver<DataType, SignalType: Origin> {
    shared: Arc<Shared<DataType, SignalType>>,
    // PhantomData<Cell<()>> forces !Sync — the poll runtime owns the
    // receiver on its thread and reads it without external sync.
    _single_consumer: PhantomData<Cell<()>>,
}

impl<DataType, SignalType> Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    /// Create a receiver bound to the poll node's `Waker`. Mint
    /// senders via [`Receiver::sender`]; the edge auto-closes when
    /// the last sender is dropped.
    pub fn new(waker: Waker) -> Self {
        let shared = Arc::new(Shared {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
                waker,
                closed: false,
            }),
            producer_count: AtomicUsize::new(0),
        });
        Self {
            shared,
            _single_consumer: PhantomData,
        }
    }

    /// Mint a new [`Sender`] connected to this receiver. Each call
    /// increments the producer refcount.
    pub fn sender(&self) -> Sender<DataType, SignalType> {
        self.shared.producer_count.fetch_add(1, Ordering::Relaxed);
        Sender {
            shared: self.shared.clone(),
        }
    }

    /// Non-blocking pop. Returns `Ok(None)` when open and empty,
    /// `Err(Closed)` when closed and empty.
    pub fn try_recv(&self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        match inner.buffer.pop_front() {
            Some(msg) => Ok(Some(msg)),
            None if inner.closed => Err(closed!()),
            None => Ok(None),
        }
    }

    /// Mark the edge closed and fire the consumer waker. Idempotent.
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        inner.waker.wake_by_ref();
        Ok(())
    }
}

impl<DataType, SignalType: Origin> Clone for Sender<DataType, SignalType> {
    fn clone(&self) -> Self {
        self.shared.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<DataType, SignalType: Origin> Drop for Sender<DataType, SignalType> {
    fn drop(&mut self) {
        if self.shared.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            close_infallible(&self.shared);
        }
    }
}

impl<DataType, SignalType: Origin> Drop for Receiver<DataType, SignalType> {
    fn drop(&mut self) {
        close_infallible(&self.shared);
    }
}

/// Close the edge and fire the waker, tolerating mutex poisoning.
///
/// Drop paths cannot panic and must still wake the consumer even if a
/// previous panic poisoned the inner mutex.
fn close_infallible<DataType, SignalType: Origin>(shared: &Shared<DataType, SignalType>) {
    let mut guard = shared
        .inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.closed = true;
    guard.waker.wake_by_ref();
}

impl<DataType, SignalType> Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    pub fn push_back(&self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        if inner.closed {
            return Err(closed!());
        }
        inner.buffer.push_back(message);
        inner.waker.wake_by_ref();
        Ok(())
    }

    /// Mark the edge closed and fire the consumer waker. Idempotent.
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        inner.waker.wake_by_ref();
        Ok(())
    }
}

impl<DataType, SignalType: Origin> Connection for Sender<DataType, SignalType> {}

impl<DataType, SignalType: Origin> Connection for Receiver<DataType, SignalType> {}

impl<DataType, SignalType> Pushable for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        self.push_back(message)
    }
}

impl<DataType, SignalType> Closeable for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn close(&mut self) -> Result<(), Error> {
        Sender::close(self)
    }
}

impl<DataType, SignalType> Receivable for Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn try_recv(&mut self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        Receiver::try_recv(self)
    }
}

impl<'params, DataType, SignalType>
    Get<dyn Pushable<DataType = DataType, SignalType = SignalType> + 'params>
    for Sender<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = DataType, SignalType = SignalType> + 'params>, Error>
    {
        Ok(Box::new(self.clone()))
    }
}

impl<'params, DataType, SignalType>
    Get<dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params>
    for Sender<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params>,
        Error,
    > {
        Ok(Box::new(self.clone()))
    }
}

// Receiver acts as a connection point: `Get::get(&receiver)` mints a
// fresh Sender so upstream nodes can push into it.
impl<'params, DataType, SignalType>
    Get<dyn Pushable<DataType = DataType, SignalType = SignalType> + 'params>
    for Receiver<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = DataType, SignalType = SignalType> + 'params>, Error>
    {
        Ok(Box::new(self.sender()))
    }
}

impl<'params, DataType, SignalType>
    Get<dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params>
    for Receiver<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params>,
        Error,
    > {
        Ok(Box::new(self.sender()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Origin;
    use crate::error::ErrorKind;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestSignal;
    impl Origin for TestSignal {}

    fn _assert_sender_send_sync() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<Sender<usize, TestSignal>>();
    }

    fn _assert_receiver_send() {
        fn require_send<T: Send>() {}
        require_send::<Receiver<usize, TestSignal>>();
    }

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
        Waker::noop().clone()
    }

    #[test]
    fn roundtrip() {
        let rx = Receiver::<usize, TestSignal>::new(noop_waker());
        let tx = rx.sender();
        tx.push_back(Message::Data(7)).unwrap();
        assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(7)));
        assert_eq!(rx.try_recv().unwrap(), None);
    }

    #[test]
    fn push_fires_waker() {
        let (waker, woken) = test_waker();
        let rx = Receiver::<usize, TestSignal>::new(waker);
        let tx = rx.sender();
        assert!(!woken.load(Ordering::SeqCst));
        tx.push_back(Message::Data(1)).unwrap();
        assert!(woken.load(Ordering::SeqCst));
    }

    #[test]
    fn dropping_last_sender_closes_and_wakes() {
        let (waker, woken) = test_waker();
        let rx = Receiver::<usize, TestSignal>::new(waker);
        let tx = rx.sender();
        drop(tx);
        assert!(woken.load(Ordering::SeqCst));
        assert!(matches!(rx.try_recv().unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn multi_sender_only_closes_on_last_drop() {
        let (waker, woken) = test_waker();
        let rx = Receiver::<usize, TestSignal>::new(waker);
        let tx1 = rx.sender();
        let tx2 = rx.sender();

        drop(tx1);
        // Still open; waker did fire on push during the drop path? No
        // — close_infallible only runs when the *last* sender drops.
        assert!(!woken.load(Ordering::SeqCst));
        tx2.push_back(Message::Data(2)).unwrap();
        assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(2)));

        woken.store(false, Ordering::SeqCst);
        drop(tx2);
        assert!(woken.load(Ordering::SeqCst));
        assert!(matches!(rx.try_recv().unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn dropping_receiver_closes_senders() {
        let (waker, _woken) = test_waker();
        let rx = Receiver::<usize, TestSignal>::new(waker);
        let tx = rx.sender();
        drop(rx);
        assert!(matches!(
            tx.push_back(Message::Data(1)).unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn explicit_close_via_sender() {
        let (waker, woken) = test_waker();
        let rx = Receiver::<usize, TestSignal>::new(waker);
        let tx = rx.sender();
        tx.close().unwrap();
        assert!(woken.load(Ordering::SeqCst));
        assert!(matches!(rx.try_recv().unwrap_err().kind, ErrorKind::Closed));
        // Idempotent.
        tx.close().unwrap();
    }

    #[test]
    fn explicit_close_via_receiver() {
        let (waker, woken) = test_waker();
        let rx = Receiver::<usize, TestSignal>::new(waker);
        let tx = rx.sender();
        rx.close().unwrap();
        assert!(woken.load(Ordering::SeqCst));
        assert!(matches!(
            tx.push_back(Message::Data(1)).unwrap_err().kind,
            ErrorKind::Closed
        ));
        // Idempotent.
        rx.close().unwrap();
    }

    #[test]
    fn buffered_data_readable_after_sender_drop() {
        let rx = Receiver::<usize, TestSignal>::new(noop_waker());
        let tx = rx.sender();
        tx.push_back(Message::Data(1)).unwrap();
        tx.push_back(Message::Data(2)).unwrap();
        drop(tx);

        assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(1)));
        assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(2)));
        assert!(matches!(rx.try_recv().unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn cross_thread_fan_in() {
        let (waker, _woken) = test_waker();
        let rx = Receiver::<usize, TestSignal>::new(waker);

        let mut handles = Vec::new();
        for i in 0..5 {
            let tx = rx.sender();
            handles.push(thread::spawn(move || {
                tx.push_back(Message::Data(i)).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut got = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(Some(Message::Data(v))) => got.push(v),
                Ok(Some(other)) => panic!("unexpected {:?}", other),
                Ok(None) => break,
                Err(_) => break,
            }
        }
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn drop_both_ends_immediately() {
        let rx = Receiver::<usize, TestSignal>::new(noop_waker());
        let tx = rx.sender();
        drop(rx);
        drop(tx);
        // No panic, no deadlock.
    }

    #[test]
    fn get_on_receiver_mints_sender() {
        let rx = Receiver::<usize, TestSignal>::new(noop_waker());
        let mut pushable: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
            Get::get(&rx).unwrap();
        pushable.push(Message::Data(9)).unwrap();
        assert_eq!(rx.try_recv().unwrap(), Some(Message::Data(9)));
    }
}
