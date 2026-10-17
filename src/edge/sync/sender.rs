//! The producer end of an edge.

use super::shared::{Inner, Shared, close_infallible};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{BoxAs, Closeable, Get, Outlet, Pushable, Schedule, TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Origin;
use crate::wake;
use crate::{closed, fatal};
use alloc::sync::Arc;
use core::mem;
use core::sync::atomic::Ordering;
use core::task::Waker;
use std::sync::MutexGuard;

/// Producer handle. `Send + Sync + Clone`. Each clone is one producer: closing or dropping it
/// says that producer is done, and the edge closes once every producer is. Closing the
/// [`Receiver`](super::Receiver) closes the edge for all of them.
///
/// Use one handle per producer: [`Sender::try_push_back`] remembers this handle's last
/// refusal, so two producers sharing one handle can see a false [`TryPush::Stuck`].
///
/// [`Schedule`]: after a [`TryPush::Full`], the scheduled waker is woken once the receiver pops
/// or the edge closes; if bound, a [`TryPush::Stuck`] is already owed that wake. A clone is
/// unbound, and an unbound `Full` records nothing.
pub struct Sender<DataType, SignalType: Origin> {
    pub(super) shared: Arc<Shared<DataType, SignalType>>,
    /// The edge's progress when this handle was last refused. Reset by [`Schedule`] (that
    /// refusal recorded no wake for the new waker); otherwise never cleared: only a drain frees
    /// room, so after any successful push progress has moved past it.
    pub(super) refused_at: Option<u64>,
    /// Recorded on each `Full`, woken by the next pop or close; `None` until scheduled. Only
    /// `.sync`: the edge crosses threads.
    pub(super) waker: Option<Waker>,
    /// This producer is done: it no longer counts toward keeping the edge open.
    pub(super) released: bool,
}

/// Wakes once, after a refused `try_push_back` (`Full`), at the next pop or close.
/// Re-scheduling replaces the waker; a refusal already recorded still wakes the old one, once.
/// Same target: no-op.
impl<DataType, SignalType> Schedule for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn schedule(&mut self, waker: wake::Waker) {
        // Same target: its refusal, if any, is already recorded; keep it, so no second entry.
        if self
            .waker
            .as_ref()
            .is_some_and(|current| current.will_wake(&waker.sync))
        {
            return;
        }
        self.waker = Some(waker.sync);
        // A refusal while unbound (or bound elsewhere) recorded nothing for this waker: forget
        // it, so the next refusal is `Full` and records, not a wakeless `Stuck`.
        self.refused_at = None;
    }
}

impl<DataType, SignalType> Clone for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn clone(&self) -> Self {
        // A done producer's clone is done too: it can't reopen what it finished.
        if !self.released {
            // Matches std::sync::Arc::clone: no synchronization needed at the
            // clone site since the new reference can only be used through the
            // new owner.
            self.shared.producer_count.fetch_add(1, Ordering::Relaxed);
        }
        // A new producer: this handle's refusals and waker aren't its.
        Self {
            shared: self.shared.clone(),
            refused_at: None,
            waker: None,
            released: self.released,
        }
    }
}

impl<DataType, SignalType: Origin> Sender<DataType, SignalType> {
    /// Stop counting as a producer, once. True when this was the last one: the caller closes.
    fn release(&mut self) -> bool {
        !mem::replace(&mut self.released, true)
            && self.shared.producer_count.fetch_sub(1, Ordering::AcqRel) == 1
    }
}

impl<DataType, SignalType: Origin> Drop for Sender<DataType, SignalType> {
    fn drop(&mut self) {
        if self.release() {
            close_infallible(&self.shared);
        }
    }
}

impl<DataType, SignalType> Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    /// Push a message onto the back of the queue. On a full bounded edge, blocks until the
    /// receiver pops or the edge closes. Returns [`crate::error::ErrorKind::Closed`] if the
    /// receiver has been dropped or the edge has been closed.
    pub fn push_back(&self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        if self.released {
            return Err(closed!());
        }
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
        mut inner: MutexGuard<'a, Inner<DataType, SignalType>>,
    ) -> Result<MutexGuard<'a, Inner<DataType, SignalType>>, Error> {
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
        message: Message<DataType, SignalType>,
    ) -> Result<TryPush<Message<DataType, SignalType>>, Error> {
        if self.released {
            return Err(closed!());
        }
        let mut inner = self.shared.inner.lock().map_err(|e| fatal!(e))?;
        if inner.closed {
            return Err(closed!());
        }
        if inner.is_full() {
            // No progress since our last refusal, so a retry would spin. Its waker, if bound, is
            // already in the list: recording again would duplicate it.
            if self.refused_at == Some(inner.progress) {
                return Ok(TryPush::Stuck(message));
            }
            self.refused_at = Some(inner.progress);
            // Recorded under the lock that saw the edge full: any later drain or close takes it.
            if let Some(waker) = &self.waker {
                inner.refused.push(waker.clone());
            }
            return Ok(TryPush::Full(message));
        }
        inner.buffer.push_back(message);
        self.wake_consumer(inner);
        Ok(TryPush::Pushed)
    }

    /// Notify the receiver only if it is blocked, after releasing the lock. The flag is set and
    /// cleared under the lock, and there is at most one waiter (`Receiver` is `!Sync`).
    fn wake_consumer(&self, mut inner: MutexGuard<'_, Inner<DataType, SignalType>>) {
        // Read and clear under the lock: the receiver sets it under the same lock before waiting.
        let consumer_waiting = mem::take(&mut inner.consumer_waiting);
        // Unlock before notifying, so the woken receiver doesn't immediately block on our lock.
        drop(inner);
        if consumer_waiting {
            self.shared.not_empty.notify_one();
        }
    }

    /// This producer is done; the edge closes once every producer is. What it pushed before
    /// stays queued for the receiver. Idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
        // Infallible like `Drop`: once released, a poisoned lock must still close the edge.
        if self.release() {
            close_infallible(&self.shared);
        }
        Ok(())
    }
}

impl<DataType, SignalType> Connection for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
}

impl<DataType, SignalType> Outlet for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;
}

impl<DataType, SignalType> TryPushable for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn try_push(
        &mut self,
        message: Message<DataType, SignalType>,
    ) -> Result<TryPush<Message<DataType, SignalType>>, Error> {
        self.try_push_back(message)
    }
}

impl<DataType, SignalType> Pushable for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
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

impl<ConnectionType, DataType, SignalType> Get<ConnectionType> for Sender<DataType, SignalType>
where
    ConnectionType: Connection + ?Sized,
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    Sender<DataType, SignalType>: BoxAs<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Ok(self.clone().box_as())
    }
}
