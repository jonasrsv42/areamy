//! Shared [crate::poll::Sink] mocks for tests.

use crate::Trackable;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Outlet, Schedule, TryPush, TryPushable};
use crate::message::Message;
use crate::wake;
use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

pub(crate) type Signal = Trackable<&'static str>;

/// A poll sink with no blocking push. Clones share state, so a boxed clone can be inspected
/// through the original.
#[derive(Clone, Default)]
pub(crate) struct ScheduleOnly {
    pub(crate) items: Arc<Mutex<Vec<Message<usize, Signal>>>>,
    pub(crate) schedules: Arc<AtomicUsize>,
    pub(crate) closed: Arc<AtomicBool>,
}

impl Connection for ScheduleOnly {}

impl Outlet for ScheduleOnly {
    type DataType = usize;
    type SignalType = Signal;
}

/// Takes everything.
impl TryPushable for ScheduleOnly {
    fn try_push(
        &mut self,
        msg: Message<usize, Signal>,
    ) -> Result<TryPush<Message<usize, Signal>>, Error> {
        self.items
            .lock()
            .map_err(|_| crate::fatal!("ScheduleOnly poisoned"))?
            .push(msg);
        Ok(TryPush::Pushed)
    }
}

impl Closeable for ScheduleOnly {
    fn close(&mut self) -> Result<(), Error> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Counts bindings; never wakes.
impl Schedule for ScheduleOnly {
    fn schedule(&mut self, _waker: wake::Waker) {
        self.schedules.fetch_add(1, Ordering::SeqCst);
    }
}
