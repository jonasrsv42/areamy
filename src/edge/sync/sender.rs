//! The producer end of an edge.

use super::shared::{Inner, Shared, close_infallible};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Get, Outlet, Pushable, TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Origin;
use crate::work::Sink;
use crate::{closed, fatal};
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::{Arc, MutexGuard};

/// Producer handle. `Send + Sync + Clone`. Dropping the last clone
/// closes the edge and wakes any blocked [`Receiver`](super::Receiver).
///
/// Use one handle per producer: [`Sender::try_push_back`] remembers this handle's last
/// refusal, so two producers sharing one handle can see a false [`TryPush::Stuck`].
pub struct Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    pub(super) shared: Arc<Shared<DataType, SignalType>>,
    /// The edge's progress when this handle was last refused. Never cleared: only a drain frees
    /// room, so after any successful push progress has moved past it.
    pub(super) refused_at: Option<u64>,
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

impl<D, S> Connection for Sender<D, S>
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
