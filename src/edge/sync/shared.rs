//! The state both ends of an edge share, behind one lock.

use crate::message::Message;
use crate::signal::Origin;
use core::task::Waker;
use std::collections::VecDeque;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex, MutexGuard};

/// How many slots a pop freed, which decides how many blocked producers to wake.
pub(super) enum Freed {
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

pub(super) struct Inner<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    pub(super) buffer: VecDeque<Message<DataType, SignalType>>,
    pub(super) closed: bool,
    /// The receiver is parked on `not_empty`; pushes only notify it when set.
    pub(super) consumer_waiting: bool,
    /// Maximum messages the buffer holds, signals included; `None` is unbounded.
    pub(super) bound: Option<NonZeroUsize>,
    /// Producers parked on `not_full`; pops only notify it when non-zero.
    pub(super) producers_waiting: usize,
    /// Consumer progress: bumped (wrapping) once per drain, a pop or a whole `read_all`; only
    /// compared for equality. A sender refused twice at the same value is
    /// [`TryPush::Stuck`](crate::graph::TryPush::Stuck).
    pub(super) progress: u64,
    /// Wakers recorded by `Full` refusals since the last drain. A bound sender is refused `Full`
    /// once per epoch (between drains) and every drain (and close) takes the whole list, so each
    /// handle's waker is in it at most once; a sender re-scheduled mid-epoch adds one entry per
    /// re-schedule (the old waker is still woken). Never deduped.
    pub(super) refused: Vec<Waker>,
}

impl<DataType, SignalType> Inner<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    pub(super) fn is_full(&self) -> bool {
        self.bound
            .is_some_and(|bound| self.buffer.len() >= bound.get())
    }
}

pub(super) struct Shared<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    pub(super) inner: Mutex<Inner<DataType, SignalType>>,
    /// Wakes the receiver when a message arrives or the edge closes.
    pub(super) not_empty: Condvar,
    /// Wakes producers blocked on a full bounded edge when a pop frees room or the edge closes.
    pub(super) not_full: Condvar,
    pub(super) producer_count: AtomicUsize,
}

/// Close the edge, tolerating a poisoned mutex.
///
/// Drop paths cannot panic and must still wake a blocked receiver even
/// if a previous panic poisoned the inner mutex.
pub(super) fn close_infallible<D, S>(shared: &Shared<D, S>)
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    let inner = shared
        .inner
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    close(shared, inner);
}

/// Every close path ends here, so none forgets a step: mark the edge closed, wake everyone
/// waiting on it, release the lock.
pub(super) fn close<D, S>(shared: &Shared<D, S>, mut inner: MutexGuard<'_, Inner<D, S>>)
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    // Set before notifying, under the lock: every woken waiter re-checks `closed` after
    // re-locking.
    inner.closed = true;
    shared.not_empty.notify_all();
    // Blocked producers must observe the close and return `Closed`.
    shared.not_full.notify_all();
    // Refused senders too: their retry finds the edge closed.
    let refused = mem::take(&mut inner.refused);
    drop(inner);
    wake_all(refused);
}

/// Wake each taken waker. Only after unlocking: a wake runs foreign code, which may retry (and
/// lock) the edge. Wakers must not panic (the rest of a taken batch would go unwoken), and must
/// not touch the edge when cloned (`Full` clones one under the edge lock).
pub(super) fn wake_all(wakers: impl IntoIterator<Item = Waker>) {
    for waker in wakers {
        waker.wake();
    }
}
