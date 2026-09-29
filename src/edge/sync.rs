//! [`Sender`] / [`Receiver`] pair backed by a blocking mutex + condvar
//! queue with refcounted close-on-drop semantics.
//!
//! [`Receiver::new`] creates a single-consumer endpoint with no senders
//! attached. Mint producers with [`Receiver::sender`]; each call
//! increments the producer refcount.
//!
//! When the last [`Sender`] is dropped, the edge auto-closes and any
//! blocked [`Receiver`] read is woken with
//! [`crate::error::ErrorKind::Closed`]. When the [`Receiver`] is
//! dropped, further [`Sender::push_back`] calls return `Closed`.
//!
//! [`Receiver::bounded`] caps how many messages the edge holds. When it is
//! full, [`Sender::push_back`] blocks until the receiver pops or the edge
//! closes, and [`Sender::try_push_back`] hands the message back.
//! Freed slots are not handed out in order: a producer that isn't parked
//! can take one ahead of a parked one, so a busy producer can starve others.

use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Get, Outlet, Pushable, TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Origin;
use crate::work::Sink;
use crate::{closed, fatal};
use std::cell::Cell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

/// How many slots a pop freed, which decides how many blocked producers to wake.
enum Freed {
    One,
    All,
}

/// What a waiter can do with an edge right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Has buffered data.
    Ready,
    /// Empty, but open with a live producer so data may still arrive.
    Idle,
    /// Empty and nothing can ever arrive: closed, or no producer left.
    Exhausted,
}

struct Inner<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    buffer: VecDeque<Message<DataType, SignalType>>,
    closed: bool,
    /// The receiver is parked on `not_empty`; pushes only notify it when set.
    consumer_waiting: bool,
    /// Maximum messages the buffer holds, signals included; `None` is unbounded.
    bound: Option<NonZeroUsize>,
    /// Producers parked on `not_full`; pops only notify it when non-zero.
    producers_waiting: usize,
    /// Consumer progress: bumped (wrapping) once per drain, a pop or a whole `read_all`; only
    /// compared for equality. A sender refused twice at the same value is [`TryPush::Stuck`].
    progress: u64,
}

impl<DataType, SignalType> Inner<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn is_full(&self) -> bool {
        self.bound
            .is_some_and(|bound| self.buffer.len() >= bound.get())
    }
}

struct Shared<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    inner: Mutex<Inner<DataType, SignalType>>,
    /// Wakes the receiver when a message arrives or the edge closes.
    not_empty: Condvar,
    /// Wakes producers blocked on a full bounded edge when a pop frees room or the edge closes.
    not_full: Condvar,
    producer_count: AtomicUsize,
}

/// Producer handle. `Send + Sync + Clone`. Dropping the last clone
/// closes the edge and wakes any blocked [`Receiver`].
///
/// Use one handle per producer: [`Sender::try_push_back`] remembers this handle's last
/// refusal, so two producers sharing one handle can see a false [`TryPush::Stuck`].
pub struct Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    shared: Arc<Shared<DataType, SignalType>>,
    /// The edge's progress when this handle was last refused. Never cleared: only a drain frees
    /// room, so after any successful push progress has moved past it.
    refused_at: Option<u64>,
}

/// Single-consumer handle. `Send + !Sync + !Clone`. Dropping the
/// receiver closes the edge so that further [`Sender::push_back`]
/// calls return `Closed`.
///
/// ```compile_fail,E0277
/// fn require_clone<T: Clone>() {}
/// require_clone::<areamy::edge::sync::Receiver<usize, areamy::Trackable<&'static str>>>();
/// ```
///
/// ```compile_fail,E0277
/// fn require_sync<T: Sync>() {}
/// require_sync::<areamy::edge::sync::Receiver<usize, areamy::Trackable<&'static str>>>();
/// ```
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
    /// Blocking reads on a receiver with no senders return `Closed`
    /// at once, since nothing could ever wake them. Mint senders
    /// before reading.
    pub fn new() -> Self {
        Self::with_bound(None)
    }

    /// Like [`Receiver::new`], but the edge holds at most `bound` messages, signals included.
    /// When full, [`Sender::push_back`] blocks and [`Sender::try_push_back`] hands the message
    /// back.
    pub fn bounded(bound: NonZeroUsize) -> Self {
        Self::with_bound(Some(bound))
    }

    fn with_bound(bound: Option<NonZeroUsize>) -> Self {
        let shared = Arc::new(Shared {
            inner: Mutex::new(Inner {
                buffer: VecDeque::new(),
                closed: false,
                consumer_waiting: false,
                bound,
                producers_waiting: 0,
                progress: 0,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
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
            refused_at: None,
        }
    }
}

impl<DataType, SignalType> Default for Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn default() -> Self {
        Self::new()
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
        // A new producer: this handle's refusals aren't its.
        Self {
            shared: self.shared.clone(),
            refused_at: None,
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
    // Set before notifying, under the lock: every woken waiter re-checks `closed` after re-locking.
    guard.closed = true;
    shared.not_empty.notify_all();
    // Blocked producers must observe the close and return `Closed`.
    shared.not_full.notify_all();
}

impl<D, S> Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// Push a message onto the back of the queue. On a full bounded edge, blocks until the
    /// receiver pops or the edge closes. Returns [`crate::error::ErrorKind::Closed`] if the
    /// receiver has been dropped or the edge has been closed.
    pub fn push_back(&self, message: Message<D, S>) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        if inner.is_full() && !inner.closed {
            inner = self.wait_for_room(inner)?;
        }
        // Checked after the wait too: close releases blocked producers.
        if inner.closed {
            return Err(closed!());
        }
        inner.buffer.push_back(message);
        self.wake_consumer(inner);
        Ok(())
    }

    /// Park until the edge has room or closes. The count tells pops to notify; `wait_while`
    /// absorbs spurious wakeups.
    fn wait_for_room<'a>(
        &'a self,
        mut inner: MutexGuard<'a, Inner<D, S>>,
    ) -> Result<MutexGuard<'a, Inner<D, S>>, Error> {
        // Count in while still holding the lock, before waiting: a pop reads the count under the
        // same lock, so it can't miss us.
        inner.producers_waiting += 1;
        // `wait_while` unlocks while parked and re-locks before each check and on return.
        let waited = self
            .shared
            .not_full
            .wait_while(inner, |inner| inner.is_full() && !inner.closed);
        // Poisoned or not, the guard comes back, so the count is always restored.
        let (mut inner, poisoned) = match waited {
            Ok(inner) => (inner, false),
            Err(poison) => (poison.into_inner(), true),
        };
        // Count out under the re-acquired lock.
        inner.producers_waiting -= 1;
        if poisoned {
            return Err(fatal!("edge lock poisoned"));
        }
        Ok(inner)
    }

    /// Push a message without ever blocking. A full bounded edge hands it back: as
    /// [`TryPush::Stuck`] if nothing popped since this handle was last refused, else as
    /// [`TryPush::Full`]. Returns `Closed` if the edge is closed, even when it is also full.
    pub fn try_push_back(
        &mut self,
        message: Message<D, S>,
    ) -> Result<TryPush<Message<D, S>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        if inner.closed {
            return Err(closed!());
        }
        if inner.is_full() {
            // No progress since our last refusal, so a retry would spin.
            if self.refused_at == Some(inner.progress) {
                return Ok(TryPush::Stuck(message));
            }
            self.refused_at = Some(inner.progress);
            return Ok(TryPush::Full(message));
        }
        inner.buffer.push_back(message);
        self.wake_consumer(inner);
        Ok(TryPush::Pushed)
    }

    /// Notify the receiver only if it is blocked, after releasing the lock. The flag is set and
    /// cleared under the lock, and there is at most one waiter (`Receiver` is `!Sync`).
    fn wake_consumer(&self, mut inner: MutexGuard<'_, Inner<D, S>>) {
        // Read and clear under the lock: the receiver sets it under the same lock before waiting.
        let consumer_waiting = mem::take(&mut inner.consumer_waiting);
        // Unlock before notifying, so the woken receiver doesn't immediately block on our lock.
        drop(inner);
        if consumer_waiting {
            self.shared.not_empty.notify_one();
        }
    }

    /// Mark the edge closed. Idempotent.
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        self.shared.not_empty.notify_all();
        // Blocked producers must observe the close and return `Closed`.
        self.shared.not_full.notify_all();
        Ok(())
    }
}

impl<D, S> Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// One-lock readout of what a waiter can do with this edge.
    pub(crate) fn state(&self) -> Result<State, Error> {
        let inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        if !inner.buffer.is_empty() {
            return Ok(State::Ready);
        }
        if inner.closed || self.shared.producer_count.load(Ordering::Acquire) == 0 {
            return Ok(State::Exhausted);
        }
        Ok(State::Idle)
    }

    /// Block until the buffer is non-empty. `Closed` if nothing could
    /// ever wake us.
    fn wait_nonempty(&self) -> Result<MutexGuard<'_, Inner<D, S>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        while inner.buffer.is_empty() {
            if inner.closed || self.shared.producer_count.load(Ordering::Acquire) == 0 {
                return Err(closed!());
            }
            // Set on every iteration: a spurious wakeup must re-arm it.
            inner.consumer_waiting = true;
            inner = self.shared.not_empty.wait(inner).map_err(|e| fatal!(e))?;
        }
        Ok(inner)
    }

    /// Block until a message is available, then pop it. `Closed` if
    /// nothing could ever wake us: closed and empty, or no producer.
    pub fn read_front(&self) -> Result<Message<D, S>, Error> {
        let mut inner = self.wait_nonempty()?;
        let Some(msg) = inner.buffer.pop_front() else {
            return fatal!("non-empty queue with no element").into();
        };
        inner.progress = inner.progress.wrapping_add(1);
        self.wake_producers(inner, Freed::One);
        Ok(msg)
    }

    /// Block until at least one message is available, then drain the
    /// whole buffer. `Closed` if nothing could ever wake us: closed and
    /// empty, or no producer.
    pub fn read_all(&self) -> Result<Vec<Message<D, S>>, Error> {
        let mut inner = self.wait_nonempty()?;
        let messages = inner.buffer.drain(..).collect();
        // One bump is enough: `Stuck` only asks whether anything drained.
        inner.progress = inner.progress.wrapping_add(1);
        self.wake_producers(inner, Freed::All);
        Ok(messages)
    }

    /// Non-blocking pop. `Ok(None)` when open and empty, `Err(Closed)`
    /// when closed and empty. Unlike the blocking reads this ignores
    /// the producer count, so an unconnected edge stays `Ok(None)`.
    pub fn poll(&self) -> Result<Option<Message<D, S>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        match inner.buffer.pop_front() {
            Some(msg) => {
                inner.progress = inner.progress.wrapping_add(1);
                self.wake_producers(inner, Freed::One);
                Ok(Some(msg))
            }
            None if inner.closed => Err(closed!()),
            None => Ok(None),
        }
    }

    /// Notify producers blocked on a full edge, after releasing the lock, only if any are
    /// parked. One freed slot wakes one producer; draining everything wakes all.
    fn wake_producers(&self, inner: MutexGuard<'_, Inner<D, S>>, freed: Freed) {
        // Read under the lock: producers count themselves in under the same lock before waiting,
        // so a parked producer is never missed.
        let producers_waiting = inner.producers_waiting > 0;
        // Unlock before notifying, so woken producers don't immediately block on our lock.
        drop(inner);
        if producers_waiting {
            match freed {
                Freed::One => self.shared.not_full.notify_one(),
                Freed::All => self.shared.not_full.notify_all(),
            }
        }
    }

    /// Block until the buffer is non-empty without popping. `Closed` if
    /// nothing could ever wake us: closed and empty, or no producer.
    pub fn wait_front(&self) -> Result<(), Error> {
        self.wait_nonempty().map(|_| ())
    }

    /// Mark the edge closed. Idempotent.
    pub fn close(&self) -> Result<(), Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        inner.closed = true;
        self.shared.not_empty.notify_all();
        // Blocked producers must observe the close and return `Closed`.
        self.shared.not_full.notify_all();
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

impl<D, S> Outlet for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    type DataType = D;
    type SignalType = S;
}

impl<D, S> TryPushable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn try_push(&mut self, message: Message<D, S>) -> Result<TryPush<Message<D, S>>, Error> {
        self.try_push_back(message)
    }
}

impl<D, S> Pushable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
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

impl<'params, D, S> Get<dyn Pushable<DataType = D, SignalType = S> + 'params> for Sender<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = D, SignalType = S> + 'params>, Error> {
        Ok(Box::new(self.clone()))
    }
}

impl<'params, D, S> Get<dyn Sink<DataType = D, SignalType = S> + Send + Sync + 'params>
    for Sender<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Sink<DataType = D, SignalType = S> + Send + Sync + 'params>, Error> {
        Ok(Box::new(self.clone()))
    }
}

// Receiver acts as a connection point in the graph: `Get::get(&receiver)`
// mints a fresh Sender so upstream nodes can push into it.
impl<'params, D, S> Get<dyn Pushable<DataType = D, SignalType = S> + 'params> for Receiver<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = D, SignalType = S> + 'params>, Error> {
        Ok(Box::new(self.sender()))
    }
}

impl<'params, D, S> Get<dyn Sink<DataType = D, SignalType = S> + Send + Sync + 'params>
    for Receiver<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Sink<DataType = D, SignalType = S> + Send + Sync + 'params>, Error> {
        Ok(Box::new(self.sender()))
    }
}

#[cfg(test)]
impl<D, S> Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// Producers currently parked on a full edge; lets tests wait for a real block.
    pub(crate) fn producers_waiting(&self) -> usize {
        self.shared
            .inner
            .lock()
            .map(|inner| inner.producers_waiting)
            .unwrap_or(0)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Trackable;
    use crate::error::ErrorKind;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    type TestSignal = Trackable<&'static str>;

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn bounded(bound: usize) -> Receiver<usize, TestSignal> {
        Receiver::bounded(NonZeroUsize::new(bound).unwrap())
    }

    /// Spin until `condition` holds; fails after `TIMEOUT` so a broken edge doesn't hang.
    pub(crate) fn wait_until(condition: impl Fn() -> bool) {
        let deadline = Instant::now() + TIMEOUT;
        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting");
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Join a thread, failing instead of hanging if it never finishes.
    pub(crate) fn join_within<T>(handle: thread::JoinHandle<T>) -> T {
        wait_until(|| handle.is_finished());
        handle.join().unwrap()
    }

    fn is_closed(result: Result<(), Error>) -> bool {
        matches!(
            result,
            Err(Error {
                kind: ErrorKind::Closed,
                ..
            })
        )
    }

    #[test]
    fn try_push_full_at_bound() {
        let rx = bounded(2);
        let mut tx = rx.sender();
        assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(tx.try_push_back(Message::Data(2)).unwrap(), TryPush::Pushed);
        assert_eq!(
            tx.try_push_back(Message::Data(3)).unwrap(),
            TryPush::Full(Message::Data(3))
        );
        assert_eq!(rx.len().unwrap(), 2);
    }

    #[test]
    fn pop_makes_room() {
        let rx = bounded(1);
        let mut tx = rx.sender();
        assert_eq!(tx.try_push_back(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(rx.read_front().unwrap(), Message::Data(1));
        assert_eq!(tx.try_push_back(Message::Data(2)).unwrap(), TryPush::Pushed);
    }

    #[test]
    fn signals_count_toward_bound() {
        let rx = bounded(1);
        let mut tx = rx.sender();
        assert_eq!(
            tx.try_push_back(Message::Flush("f".into())).unwrap(),
            TryPush::Pushed
        );
        assert_eq!(
            tx.try_push_back(Message::Data(1)).unwrap(),
            TryPush::Full(Message::Data(1))
        );
    }

    #[test]
    fn refused_again_without_pop_is_stuck() {
        let rx = bounded(1);
        let mut tx = rx.sender();
        tx.push_back(Message::Data(1)).unwrap();
        assert_eq!(
            tx.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
        assert_eq!(
            tx.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Stuck(Message::Data(2))
        );
    }

    /// Refused, then `pop` frees a slot that another producer refills: the retry must be `Full`
    /// (something drained), and a further retry `Stuck` (nothing drained since).
    fn pop_between_refusals_gives_full(pop: impl Fn(&Receiver<usize, TestSignal>)) {
        let rx = bounded(1);
        let mut tx = rx.sender();
        let other = rx.sender();
        tx.push_back(Message::Data(1)).unwrap();
        assert_eq!(
            tx.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
        pop(&rx);
        other.push_back(Message::Data(3)).unwrap();
        assert_eq!(
            tx.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
        assert_eq!(
            tx.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Stuck(Message::Data(2))
        );
    }

    #[test]
    fn read_front_counts_as_progress() {
        pop_between_refusals_gives_full(|rx| {
            rx.read_front().unwrap();
        });
    }

    #[test]
    fn poll_counts_as_progress() {
        pop_between_refusals_gives_full(|rx| {
            rx.poll().unwrap();
        });
    }

    #[test]
    fn read_all_counts_as_progress() {
        pop_between_refusals_gives_full(|rx| {
            rx.read_all().unwrap();
        });
    }

    #[test]
    fn each_sender_tracks_its_own_refusals() {
        let rx = bounded(1);
        let (mut a, mut b) = (rx.sender(), rx.sender());
        a.push_back(Message::Data(1)).unwrap();
        assert_eq!(
            a.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
        // B's first refusal, though A was just refused at the same count.
        assert_eq!(
            b.try_push_back(Message::Data(3)).unwrap(),
            TryPush::Full(Message::Data(3))
        );
        // A clone is a new producer, not A.
        let mut c = a.clone();
        assert_eq!(
            c.try_push_back(Message::Data(4)).unwrap(),
            TryPush::Full(Message::Data(4))
        );
        assert_eq!(
            a.try_push_back(Message::Data(2)).unwrap(),
            TryPush::Stuck(Message::Data(2))
        );
    }

    #[test]
    fn closed_beats_full() {
        let rx = bounded(1);
        let mut tx = rx.sender();
        tx.push_back(Message::Data(1)).unwrap();
        rx.close().unwrap();
        assert!(matches!(
            tx.try_push_back(Message::Data(2)),
            Err(Error {
                kind: ErrorKind::Closed,
                ..
            })
        ));
    }

    #[test]
    fn blocking_push_waits_for_read_front() {
        let rx = bounded(1);
        // Kept alive: dropping the last sender would close the edge.
        let first = rx.sender();
        first.push_back(Message::Data(1)).unwrap();
        let tx = rx.sender();
        let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

        wait_until(|| rx.producers_waiting() == 1);
        // Still blocked: the second message isn't queued yet.
        assert_eq!(rx.len().unwrap(), 1);

        assert_eq!(rx.read_front().unwrap(), Message::Data(1));
        join_within(producer).unwrap();
        // Counted out: a stale count would make every later pop notify.
        assert_eq!(rx.producers_waiting(), 0);
        assert_eq!(rx.read_front().unwrap(), Message::Data(2));
    }

    #[test]
    fn poll_wakes_blocked_producer() {
        let rx = bounded(1);
        // Kept alive: dropping the last sender would close the edge.
        let first = rx.sender();
        first.push_back(Message::Data(1)).unwrap();
        let tx = rx.sender();
        let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

        wait_until(|| rx.producers_waiting() == 1);
        assert_eq!(rx.poll().unwrap(), Some(Message::Data(1)));
        join_within(producer).unwrap();
        assert_eq!(rx.poll().unwrap(), Some(Message::Data(2)));
    }

    #[test]
    fn read_all_wakes_every_blocked_producer() {
        let rx = bounded(2);
        let tx = rx.sender();
        tx.push_back(Message::Data(1)).unwrap();
        tx.push_back(Message::Data(2)).unwrap();
        let (tx3, tx4) = (rx.sender(), rx.sender());
        let first = thread::spawn(move || tx3.push_back(Message::Data(3)));
        let second = thread::spawn(move || tx4.push_back(Message::Data(4)));

        wait_until(|| rx.producers_waiting() == 2);
        assert_eq!(
            rx.read_all().unwrap(),
            vec![Message::Data(1), Message::Data(2)]
        );
        // Both freed slots are taken without any further pop.
        join_within(first).unwrap();
        join_within(second).unwrap();

        let mut rest = Message::data_from_iter(rx.read_all().unwrap().into_iter());
        rest.sort();
        assert_eq!(rest, vec![3, 4]);
    }

    #[test]
    fn receiver_close_releases_blocked_producer() {
        let rx = bounded(1);
        // Kept alive: dropping the last sender would close the edge.
        let first = rx.sender();
        first.push_back(Message::Data(1)).unwrap();
        let tx = rx.sender();
        let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

        wait_until(|| rx.producers_waiting() == 1);
        rx.close().unwrap();
        assert!(is_closed(join_within(producer)));
    }

    #[test]
    fn receiver_drop_releases_blocked_producer() {
        let rx = bounded(1);
        // Kept alive: dropping the last sender would close the edge.
        let first = rx.sender();
        first.push_back(Message::Data(1)).unwrap();
        let tx = rx.sender();
        let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

        wait_until(|| rx.producers_waiting() == 1);
        drop(rx);
        assert!(is_closed(join_within(producer)));
    }

    #[test]
    fn sender_close_releases_other_blocked_producer() {
        let rx = bounded(1);
        let closer = rx.sender();
        closer.push_back(Message::Data(1)).unwrap();
        let tx = rx.sender();
        let producer = thread::spawn(move || tx.push_back(Message::Data(2)));

        wait_until(|| rx.producers_waiting() == 1);
        closer.close().unwrap();
        assert!(is_closed(join_within(producer)));
    }

    #[test]
    fn bound_holds_under_concurrent_producers() {
        const PRODUCERS: usize = 4;
        const PER_PRODUCER: usize = 1000;
        let rx = bounded(3);
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|id| {
                let tx = rx.sender();
                thread::spawn(move || {
                    for seq in 0..PER_PRODUCER {
                        tx.push_back(Message::Data(id * PER_PRODUCER + seq))
                            .unwrap();
                    }
                })
            })
            .collect();

        // On its own thread so a stuck edge fails the test via `join_within` instead of hanging.
        // Ends with Closed once every producer has finished and dropped its sender.
        let consumer = thread::spawn(move || {
            let mut next = [0; PRODUCERS];
            let mut received = 0;
            loop {
                assert!(rx.len().unwrap() <= 3);
                let Ok(message) = rx.read_front() else { break };
                let value = message.data().unwrap();
                let (id, seq) = (value / PER_PRODUCER, value % PER_PRODUCER);
                // Each producer's messages arrive once, in order.
                assert_eq!(seq, next[id]);
                next[id] += 1;
                received += 1;
            }
            received
        });
        assert_eq!(join_within(consumer), PRODUCERS * PER_PRODUCER);
        for producer in producers {
            join_within(producer);
        }
    }

    #[test]
    fn unbounded_never_full() {
        let rx = Receiver::<usize, TestSignal>::new();
        let mut tx = rx.sender();
        for value in 0..10_000 {
            assert_eq!(
                tx.try_push_back(Message::Data(value)).unwrap(),
                TryPush::Pushed
            );
        }
        assert_eq!(rx.len().unwrap(), 10_000);
    }

    #[test]
    fn unbounded_sender_always_pushes() {
        let rx = Receiver::<usize, TestSignal>::new();
        let mut tx = rx.sender();
        assert_eq!(tx.try_push(Message::Data(5)).unwrap(), TryPush::Pushed);
        assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
    }

    fn _assert_sender_send_sync() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<Sender<usize, TestSignal>>();
    }

    fn _assert_receiver_send() {
        fn require_send<T: Send>() {}
        require_send::<Receiver<usize, TestSignal>>();
    }

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
        let mut closeable: Box<dyn Sink<DataType = usize, SignalType = TestSignal>> = Box::new(tx);
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
    fn dyn_sink_try_push_hands_back_on_full_bounded_edge() {
        let rx = bounded(1);
        let mut sink: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Get::get(&rx).unwrap();
        // On its own thread so a `try_push` that blocks fails via `join_within` instead of hanging.
        let pusher = thread::spawn(move || {
            let pushed = sink.try_push(Message::Data(1)).unwrap();
            let full = sink.try_push(Message::Data(2)).unwrap();
            (pushed, full)
        });
        assert_eq!(
            join_within(pusher),
            (TryPush::Pushed, TryPush::Full(Message::Data(2)))
        );
    }

    #[test]
    fn get_dyn_closeable_returns_working_handle() {
        let rx = Receiver::<usize, TestSignal>::new();
        let tx = rx.sender();
        let handle: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
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
