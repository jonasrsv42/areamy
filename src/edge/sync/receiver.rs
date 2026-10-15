//! The consumer end of an edge.

use super::sender::Sender;
use super::shared::{Freed, Inner, Shared, State, close_infallible};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Get, Pushable};
use crate::message::Message;
use crate::signal::Origin;
use crate::work::Sink;
use crate::{closed, fatal};
use std::cell::Cell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

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

impl<D, S> Drop for Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn drop(&mut self) {
        close_infallible(&self.shared);
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

impl<D, S> Connection for Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
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
