use super::Input;
use crate::error::Error;
use crate::fatal;
use crate::poll::future::waiters::Waiters;
use crate::poll::timer::Timer;
use crate::wake::LocalWaker;
use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::num::NonZeroUsize;

pub(super) struct Inner<T> {
    pub(super) buffer: VecDeque<Input<T>>,
    /// A Flush was received; the segment's queue is done.
    pub(super) closed: bool,
    /// A send was refused; the next pop wakes Input.
    pub(super) refused: bool,
    /// Parked `recv`s.
    pub(super) waiters: Waiters,
}

/// The state both ends share. Only `inner` is a `RefCell`, never borrowed across a wake.
pub(super) struct Shared<T> {
    inner: RefCell<Inner<T>>,
    pub(super) bound: Option<NonZeroUsize>,
    /// The node's Input phase, woken once room frees after a refusal.
    pub(super) input: LocalWaker,
    /// Re-polls the routine at a [recv_with_timeout](super::Receiver::recv_with_timeout) deadline.
    pub(super) timer: Timer,
}

impl<T> Shared<T> {
    pub(super) fn new(
        input: LocalWaker,
        timer: Timer,
        bound: Option<NonZeroUsize>,
        waiters: Waiters,
    ) -> Self {
        Self {
            inner: RefCell::new(Inner {
                buffer: VecDeque::new(),
                closed: false,
                refused: false,
                waiters,
            }),
            bound,
            input,
            timer,
        }
    }

    pub(super) fn borrow(&self) -> Result<RefMut<'_, Inner<T>>, Error> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| fatal!("input queue borrowed reentrantly"))
    }
}
