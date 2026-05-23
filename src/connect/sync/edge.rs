//! [`Sender`] / [`Receiver`] pair backed by a blocking mutex + condvar
//! queue with refcounted close-on-drop semantics.

use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{Closeable, Pushable, closed, fatal, graph::Get};
use std::cell::Cell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

struct Inner<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    buffer: VecDeque<Message<DataType, SignalType>>,
    closed: bool,
}

struct Shared<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    inner: Mutex<Inner<DataType, SignalType>>,
    signal: Condvar,
    producer_count: AtomicUsize,
}

/// Producer handle. `Send + Sync + Clone`. Dropping the last clone
/// closes the edge and wakes any blocked [`Receiver`].
pub struct Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    shared: Arc<Shared<DataType, SignalType>>,
}

/// Single-consumer handle. `Send + !Sync + !Clone`. Dropping the
/// receiver closes the edge so that further [`Sender::push_back`]
/// calls return `Closed`.
pub struct Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    shared: Arc<Shared<DataType, SignalType>>,
    // PhantomData<Cell<()>> is Send + !Sync — prevents concurrent reads
    // racing on the blocking dequeue.
    _single_consumer: PhantomData<Cell<()>>,
}

impl<DataType, SignalType> Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    /// Create a receiver with no senders attached. Mint senders via
    /// [`Receiver::sender`]. The edge auto-closes when all minted
    /// senders are dropped.
    ///
    /// A receiver that never mints a sender has nothing that can
    /// wake it, so [`read_front`](Self::read_front) /
    /// [`read_all`](Self::read_all) / [`wait_front`](Self::wait_front)
    /// will block forever — mint at least one sender before blocking,
    /// or call [`close`](Self::close) to unblock.
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
                closed: false,
            }),
            signal: Condvar::new(),
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
}

impl<D, S> Clone for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn clone(&self) -> Self {
        // Matches std::sync::Arc::clone: no synchronization needed at the
        // clone site since the new reference can only be used through the
        // new owner.
        self.shared.producer_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<D, S> Drop for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn drop(&mut self) {
        if self.shared.producer_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            close_infallible(&self.shared);
        }
    }
}

impl<D, S> Drop for Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn drop(&mut self) {
        close_infallible(&self.shared);
    }
}

/// Close the edge, tolerating a poisoned mutex.
///
/// Drop paths cannot panic and must still wake a blocked receiver even
/// if a previous panic poisoned the inner mutex.
fn close_infallible<D, S>(shared: &Shared<D, S>)
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    let mut guard = shared
        .inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.closed = true;
    shared.signal.notify_all();
}

impl<D, S> Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// Push a message onto the back of the queue. Returns
    /// [`crate::error::ErrorKind::Closed`] if the receiver has been
    /// dropped or the edge has been closed.
    pub fn push_back(&self, message: Message<D, S>) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        if inner.closed {
            return Err(closed!());
        }
        inner.buffer.push_back(message);
        self.shared.signal.notify_one();
        Ok(())
    }

    /// Push multiple messages onto the back of the queue. Returns
    /// `Closed` if the edge is closed; messages are not enqueued in
    /// that case.
    pub fn push_back_all(&self, items: Vec<Message<D, S>>) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        if inner.closed {
            return Err(closed!());
        }
        for item in items {
            inner.buffer.push_back(item);
        }
        self.shared.signal.notify_one();
        Ok(())
    }

    /// Mark the edge closed. Idempotent.
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        self.shared.signal.notify_all();
        Ok(())
    }
}

impl<D, S> Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// Block until a message is available, then pop it. Returns
    /// `Closed` if the edge is closed and the buffer is empty.
    pub fn read_front(&self) -> Result<Message<D, S>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        while inner.buffer.is_empty() {
            if inner.closed {
                return Err(closed!());
            }
            inner = self.shared.signal.wait(inner).map_err(|e| fatal!(e))?;
        }
        match inner.buffer.pop_front() {
            Some(msg) => Ok(msg),
            None => fatal!("non-empty queue with no element").into(),
        }
    }

    /// Block until at least one message is available, then drain the
    /// whole buffer. Returns `Closed` if the edge is closed and the
    /// buffer is empty.
    pub fn read_all(&self) -> Result<Vec<Message<D, S>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        while inner.buffer.is_empty() {
            if inner.closed {
                return Err(closed!());
            }
            inner = self.shared.signal.wait(inner).map_err(|e| fatal!(e))?;
        }
        Ok(inner.buffer.drain(..).collect())
    }

    /// Non-blocking pop. Returns `Ok(None)` when open and empty,
    /// `Err(Closed)` when closed and empty.
    pub fn poll(&self) -> Result<Option<Message<D, S>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        match inner.buffer.pop_front() {
            Some(msg) => Ok(Some(msg)),
            None if inner.closed => Err(closed!()),
            None => Ok(None),
        }
    }

    /// Block until the buffer is non-empty without popping. Returns
    /// `Closed` if the edge is closed while the buffer is empty.
    pub fn wait_front(&self) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        while inner.buffer.is_empty() {
            if inner.closed {
                return Err(closed!());
            }
            inner = self.shared.signal.wait(inner).map_err(|e| fatal!(e))?;
        }
        Ok(())
    }

    /// Mark the edge closed. Idempotent.
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        self.shared.signal.notify_all();
        Ok(())
    }

    pub fn len(&self) -> Result<usize, Error> {
        let inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        Ok(inner.buffer.len())
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.len()? == 0)
    }
}

impl<D, S> Connection for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
}

impl<D, S> Connection for Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
}

impl<D, S> Pushable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    type DataType = D;
    type SignalType = S;

    fn push(&mut self, message: Message<D, S>) -> Result<(), Error> {
        self.push_back(message)
    }
}

impl<D, S> Closeable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn close(&mut self) -> Result<(), Error> {
        Sender::close(self)
    }
}

impl<D, S> Get<dyn Pushable<DataType = D, SignalType = S>> for Sender<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = D, SignalType = S>>, Error> {
        Ok(Box::new(self.clone()))
    }
}

impl<D, S> Get<dyn Closeable<DataType = D, SignalType = S> + Send + Sync> for Sender<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Closeable<DataType = D, SignalType = S> + Send + Sync>, Error> {
        Ok(Box::new(self.clone()))
    }
}

// Receiver acts as a connection point in the graph: `Get::get(&receiver)`
// mints a fresh Sender so upstream nodes can push into it.
impl<D, S> Get<dyn Pushable<DataType = D, SignalType = S>> for Receiver<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = D, SignalType = S>>, Error> {
        Ok(Box::new(self.sender()))
    }
}

impl<D, S> Get<dyn Closeable<DataType = D, SignalType = S> + Send + Sync> for Receiver<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Closeable<DataType = D, SignalType = S> + Send + Sync>, Error> {
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
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestSignal(u32);
    impl Origin for TestSignal {}

    fn _assert_sender_send_sync() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<Sender<usize, TestSignal>>();
    }

    fn _assert_receiver_send() {
        fn require_send<T: Send>() {}
        require_send::<Receiver<usize, TestSignal>>();
    }

    /// ```compile_fail
    /// fn require_clone<T: Clone>() {}
    /// require_clone::<areamy::connect::sync::Receiver<usize, &'static str>>();
    /// ```
    fn _receiver_not_clone() {}

    /// ```compile_fail
    /// fn require_sync<T: Sync>() {}
    /// require_sync::<areamy::connect::sync::Receiver<usize, &'static str>>();
    /// ```
    fn _receiver_not_sync() {}

    #[test]
    fn roundtrip() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        tx.push_back(Message::Data(42)).unwrap();
        assert_eq!(rx.read_front().unwrap(), Message::Data(42));
    }

    #[test]
    fn dropping_last_sender_closes_receiver() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        drop(tx);
        assert!(matches!(
            rx.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn dropping_last_sender_unblocks_waiting_receiver() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();

        let received = Arc::new(AtomicBool::new(false));
        let received_clone = received.clone();

        let reader = thread::spawn(move || {
            let result = rx.read_front();
            received_clone.store(true, Ordering::SeqCst);
            result
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!received.load(Ordering::SeqCst));

        drop(tx);

        let result = reader.join().unwrap();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn dropping_receiver_closes_sender() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        drop(rx);
        assert!(matches!(
            tx.push_back(Message::Data(1)).unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn multi_sender_only_closes_on_last_drop() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let tx2 = tx.clone();
        let tx3 = tx.clone();

        drop(tx);
        drop(tx2);

        tx3.push_back(Message::Data(7)).unwrap();
        assert_eq!(rx.read_front().unwrap(), Message::Data(7));

        drop(tx3);

        assert!(matches!(
            rx.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn clone_does_not_close_prematurely() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        for _ in 0..10 {
            let cloned = tx.clone();
            drop(cloned);
        }
        tx.push_back(Message::Data(1)).unwrap();
        assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    }

    #[test]
    fn explicit_close_on_sender() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        tx.close().unwrap();
        assert!(matches!(
            rx.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
        tx.close().unwrap();
    }

    #[test]
    fn explicit_close_on_receiver() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        rx.close().unwrap();
        assert!(matches!(
            tx.push_back(Message::Data(1)).unwrap_err().kind,
            ErrorKind::Closed
        ));
        rx.close().unwrap();
    }

    #[test]
    fn buffered_data_readable_after_sender_drop() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        tx.push_back(Message::Data(1)).unwrap();
        tx.push_back(Message::Data(2)).unwrap();
        drop(tx);

        assert_eq!(rx.read_front().unwrap(), Message::Data(1));
        assert_eq!(rx.read_front().unwrap(), Message::Data(2));
        assert!(matches!(
            rx.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn poll_returns_none_when_open_and_empty() {
        let rx = Receiver::<usize, TestSignal>::new();
        let _tx = rx.sender();
        assert_eq!(rx.poll().unwrap(), None);
    }

    #[test]
    fn poll_returns_closed_after_sender_drop_and_drain() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        tx.push_back(Message::Data(1)).unwrap();
        drop(tx);

        assert_eq!(rx.poll().unwrap(), Some(Message::Data(1)));
        assert!(matches!(rx.poll().unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn pushable_trait_round_trips() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let mut pushable: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
            Box::new(tx);
        pushable.push(Message::Data(11)).unwrap();
        assert_eq!(rx.read_front().unwrap(), Message::Data(11));
    }

    #[test]
    fn closeable_trait_closes_edge() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let mut closeable: Box<dyn Closeable<DataType = usize, SignalType = TestSignal>> =
            Box::new(tx);
        closeable.push(Message::Data(1)).unwrap();
        Closeable::close(closeable.as_mut()).unwrap();

        assert_eq!(rx.read_front().unwrap(), Message::Data(1));
        assert!(matches!(
            rx.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn get_dyn_pushable_returns_working_handle() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let mut handle: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
            Get::get(&tx).unwrap();
        handle.push(Message::Data(3)).unwrap();
        assert_eq!(rx.read_front().unwrap(), Message::Data(3));
    }

    #[test]
    fn get_dyn_closeable_returns_working_handle() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let handle: Box<dyn Closeable<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Get::get(&tx).unwrap();
        // Original tx still alive — handle is an additional clone.
        drop(handle);
        // tx still alive, edge still open.
        tx.push_back(Message::Data(1)).unwrap();
        assert_eq!(rx.read_front().unwrap(), Message::Data(1));
    }

    #[test]
    fn dropping_boxed_pushable_decrements_count() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let handle: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
            Get::get(&tx).unwrap();
        drop(tx);
        assert_eq!(rx.poll().unwrap(), None);
        drop(handle);
        assert!(matches!(
            rx.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn wait_front_unblocked_by_last_sender_drop() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();

        let reached = Arc::new(AtomicBool::new(false));
        let reached_clone = reached.clone();

        let waiter = thread::spawn(move || {
            let result = rx.wait_front();
            reached_clone.store(true, Ordering::SeqCst);
            result
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!reached.load(Ordering::SeqCst));

        drop(tx);

        let result = waiter.join().unwrap();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn read_all_unblocked_by_last_sender_drop() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();

        let reached = Arc::new(AtomicBool::new(false));
        let reached_clone = reached.clone();

        let reader = thread::spawn(move || {
            let result = rx.read_all();
            reached_clone.store(true, Ordering::SeqCst);
            result
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!reached.load(Ordering::SeqCst));

        drop(tx);

        let result = reader.join().unwrap();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn push_back_after_explicit_sender_close_returns_closed() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        tx.close().unwrap();
        assert!(matches!(
            tx.push_back(Message::Data(1)).unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn push_back_all_after_receiver_drop_returns_closed() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        drop(rx);
        let items = vec![Message::Data(1), Message::Data(2)];
        assert!(matches!(
            tx.push_back_all(items).unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn receiver_moved_cross_thread_reads() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();

        let reader = thread::spawn(move || {
            let mut got = Vec::new();
            loop {
                match rx.read_front() {
                    Ok(Message::Data(v)) => got.push(v),
                    Err(e) if matches!(e.kind, ErrorKind::Closed) => break,
                    other => panic!("unexpected {:?}", other),
                }
            }
            got
        });

        for i in 0..3 {
            tx.push_back(Message::Data(i)).unwrap();
        }
        drop(tx);

        let got = reader.join().unwrap();
        assert_eq!(got, vec![0, 1, 2]);
    }

    #[test]
    fn receiver_first_then_last_sender_drop_is_safe() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        drop(rx);
        // Edge already marked closed by receiver drop. The sender's
        // last-drop close_infallible runs against an already-closed
        // state and must not panic or deadlock.
        drop(tx);
    }

    #[test]
    fn drop_both_ends_immediately() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        drop(rx);
        drop(tx);
        // Constructing and tearing down a channel with no traffic
        // must not panic, leak, or deadlock.
    }

    #[test]
    fn get_on_receiver_mints_sender() {
        let rx = Receiver::<usize, TestSignal>::new();
        let mut pushable: Box<dyn Pushable<DataType = usize, SignalType = TestSignal>> =
            Get::get(&rx).unwrap();
        pushable.push(Message::Data(9)).unwrap();
        assert_eq!(rx.read_front().unwrap(), Message::Data(9));
        drop(pushable);
        assert!(matches!(
            rx.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn cross_thread_fan_in() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let mut handles = Vec::new();
        for i in 0..5 {
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                tx.push_back(Message::Data(i)).unwrap();
            }));
        }
        drop(tx);

        for h in handles {
            h.join().unwrap();
        }

        let mut got = Vec::new();
        loop {
            match rx.read_front() {
                Ok(Message::Data(v)) => got.push(v),
                Err(e) if matches!(e.kind, ErrorKind::Closed) => break,
                other => panic!("unexpected {:?}", other),
            }
        }
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }
}
