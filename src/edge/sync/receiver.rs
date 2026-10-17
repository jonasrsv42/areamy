//! The consumer end of an edge.

use super::sender::Sender;
use super::shared::{Freed, Inner, Shared, State, close, close_infallible, wake_all};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{BoxAs, Get, Inlet};
use crate::message::Message;
use crate::signal::Origin;
use crate::{closed, fatal};
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use core::cell::Cell;
use core::marker::PhantomData;
use core::mem;
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::Waker;
use std::sync::{Condvar, Mutex, MutexGuard};

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
pub struct Receiver<DataType, SignalType: Origin> {
    shared: Arc<Shared<DataType, SignalType>>,
    /// Empty spare traded with the edge's refused list when a drain takes it, so neither
    /// reallocates. Out of the cell while waking: a re-entrant drain finds it empty.
    scratch: Cell<Vec<Waker>>,
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
    ///
    /// Unbounded: a bound needs the topology rule in [crate::work], so it is opt-in per edge.
    pub fn new() -> Self {
        Self::with_bound(None)
    }

    /// Like [`Receiver::new`], but the edge holds at most `bound` messages, signals included.
    /// When full, [`Sender::push_back`] blocks and [`Sender::try_push_back`] hands the message
    /// back. Exact, on one thread too: overshooting would grow without bound, silently.
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
                refused: Vec::new(),
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            producer_count: AtomicUsize::new(0),
        });
        Self {
            shared,
            scratch: Cell::new(Vec::new()),
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
            waker: None,
            released: false,
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

impl<DataType, SignalType: Origin> Drop for Receiver<DataType, SignalType> {
    fn drop(&mut self) {
        close_infallible(&self.shared);
    }
}

impl<DataType, SignalType> Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
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
    fn wait_nonempty(&self) -> Result<MutexGuard<'_, Inner<DataType, SignalType>>, Error> {
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
    pub fn read_front(&self) -> Result<Message<DataType, SignalType>, Error> {
        let mut inner = self.wait_nonempty()?;
        let Some(msg) = inner.buffer.pop_front() else {
            return fatal!("non-empty queue with no element").into();
        };
        self.drained(inner, Freed::One);
        Ok(msg)
    }

    /// Block until at least one message is available, then drain the
    /// whole buffer. `Closed` if nothing could ever wake us: closed and
    /// empty, or no producer.
    pub fn read_all(&self) -> Result<Vec<Message<DataType, SignalType>>, Error> {
        let mut inner = self.wait_nonempty()?;
        let messages = inner.buffer.drain(..).collect();
        // One drain, one bump: `Stuck` only asks whether anything drained.
        self.drained(inner, Freed::All);
        Ok(messages)
    }

    /// Non-blocking pop. `Ok(None)` when open and empty, `Err(Closed)`
    /// when closed and empty. Unlike the blocking reads this ignores
    /// the producer count, so an unconnected edge stays `Ok(None)`.
    pub fn poll(&self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        match inner.buffer.pop_front() {
            Some(msg) => {
                self.drained(inner, Freed::One);
                Ok(Some(msg))
            }
            None if inner.closed => Err(closed!()),
            None => Ok(None),
        }
    }

    /// Non-blocking pop that, like the blocking reads, reports `Closed` once the edge is empty
    /// with no producer left: nothing can ever arrive. For consumers that only poll, where an
    /// input nobody writes to must still count as done.
    pub(crate) fn try_pop(&self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        match inner.buffer.pop_front() {
            Some(msg) => {
                self.drained(inner, Freed::One);
                Ok(Some(msg))
            }
            None if inner.closed || self.shared.producer_count.load(Ordering::Acquire) == 0 => {
                Err(closed!())
            }
            None => Ok(None),
        }
    }

    /// Every pop ends here: bump `progress` and take this epoch's refused wakers in one critical
    /// section, so the list only ever holds this epoch's refusals. Then, after releasing the
    /// lock, notify blocked producers (one freed slot wakes one blocked thread, draining
    /// everything wakes all) and wake every refused waker.
    ///
    /// Wakers are all woken, threads are not: a blocked `push_back` is sure to retry, a waker
    /// only asks, so a slot handed to one could strand. Not wake-one plus wake-all once empty: a
    /// consumer that keeps a backlog never pops it empty. No low watermark either: backpressure
    /// is rare, so a wake per pop beats batching.
    fn drained(&self, mut inner: MutexGuard<'_, Inner<DataType, SignalType>>, freed: Freed) {
        inner.progress = inner.progress.wrapping_add(1);
        // Read under the lock: producers count themselves in under the same lock before waiting,
        // so a parked producer is never missed.
        let producers_waiting = inner.producers_waiting > 0;
        // Out of line, so the common pop (nobody refused) stays small enough to inline.
        if !inner.refused.is_empty() {
            return self.drained_refused(inner, producers_waiting, freed);
        }
        // Unlock before notifying, so woken producers don't immediately block on our lock.
        drop(inner);
        self.notify_producers(producers_waiting, freed);
    }

    /// [`Self::drained`] when someone was refused this epoch: still in the critical section,
    /// take the list.
    #[cold]
    #[inline(never)]
    fn drained_refused(
        &self,
        mut inner: MutexGuard<'_, Inner<DataType, SignalType>>,
        producers_waiting: bool,
        freed: Freed,
    ) {
        // Trade the list for the empty scratch, so both keep their capacity.
        let mut refused = mem::replace(&mut inner.refused, self.scratch.take());
        // Unlock before waking, so woken producers don't immediately block on our lock.
        drop(inner);
        self.notify_producers(producers_waiting, freed);
        wake_all(refused.drain(..));
        // Emptied, back as the next drain's scratch.
        self.scratch.set(refused);
    }

    /// One freed slot wakes one blocked thread, draining everything wakes all; nothing if none
    /// is parked.
    fn notify_producers(&self, producers_waiting: bool, freed: Freed) {
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
        let inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        close(&self.shared, inner);
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

impl<DataType, SignalType> Connection for Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
}

impl<DataType, SignalType> Inlet for Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;
}

// Receiver acts as a connection point in the graph: `Get::get(&receiver)`
// mints a fresh Sender so upstream nodes can push into it.
impl<ConnectionType, DataType, SignalType> Get<ConnectionType> for Receiver<DataType, SignalType>
where
    ConnectionType: Connection + ?Sized,
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    Sender<DataType, SignalType>: BoxAs<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Ok(self.sender().box_as())
    }
}

#[cfg(test)]
impl<DataType, SignalType> Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
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
