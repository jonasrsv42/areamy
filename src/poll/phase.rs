//! [Phase] pairs what a poll node phase drives with the waker that schedules it.

use crate::poll::waker::ThreadLocalWaker;

/// A phase's target (edge, routine, ...) and its waker.
pub struct Phase<T> {
    pub target: T,
    pub waker: ThreadLocalWaker,
}
