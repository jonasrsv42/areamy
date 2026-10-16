use crate::error::Error;
use crate::fatal;
use crate::poll::future::waiters::Waiters;
use crate::poll::timer::Timer;
use crate::wake::LocalWaker;
use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::num::NonZeroUsize;

pub(super) struct Inner<T> {
    pub(super) buffer: VecDeque<T>,
    /// Output's last pop found the queue empty; the next send wakes it. Its other stops (budget,
    /// a parked edge) wake it themselves.
    pub(super) armed: bool,
    /// Parked `send`s.
    pub(super) waiters: Waiters,
}

/// The state both ends share. Only `inner` is a `RefCell`, never borrowed across a wake.
pub(super) struct Shared<T> {
    inner: RefCell<Inner<T>>,
    pub(super) bound: Option<NonZeroUsize>,
    /// The node's Output phase.
    pub(super) output: LocalWaker,
    /// Re-polls the routine at a [send_with_timeout](super::Sender::send_with_timeout) deadline.
    pub(super) timer: Timer,
}

impl<T> Shared<T> {
    pub(super) fn new(output: LocalWaker, timer: Timer, bound: Option<NonZeroUsize>) -> Self {
        Self {
            inner: RefCell::new(Inner {
                buffer: VecDeque::new(),
                armed: true,
                waiters: Waiters::default(),
            }),
            bound,
            output,
            timer,
        }
    }

    pub(super) fn borrow(&self) -> Result<RefMut<'_, Inner<T>>, Error> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| fatal!("output queue borrowed reentrantly"))
    }
}
