//! Shared [crate::poll::Sink] mocks for tests.

use crate::Trackable;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Outlet, TryPush, TryPushable};
use crate::message::Message;
use crate::poll::Room;
use crate::poll::waker::Waker;
use core::task::Poll;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) type Signal = Trackable<&'static str>;

/// A poll sink with no blocking push. Clones share state, so a boxed clone can be inspected
/// through the original.
#[derive(Clone, Default)]
pub(crate) struct RoomOnly {
    pub(crate) items: Arc<Mutex<Vec<Message<usize, Signal>>>>,
    pub(crate) polls: Arc<AtomicUsize>,
    pub(crate) closed: Arc<AtomicBool>,
}

impl Connection for RoomOnly {}

impl Outlet for RoomOnly {
    type DataType = usize;
    type SignalType = Signal;
}

/// Takes everything.
impl TryPushable for RoomOnly {
    fn try_push(
        &mut self,
        msg: Message<usize, Signal>,
    ) -> Result<TryPush<Message<usize, Signal>>, Error> {
        self.items
            .lock()
            .map_err(|_| crate::fatal!("RoomOnly poisoned"))?
            .push(msg);
        Ok(TryPush::Pushed)
    }
}

impl Closeable for RoomOnly {
    fn close(&mut self) -> Result<(), Error> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Counts polls; always `Pending`.
impl Room for RoomOnly {
    fn poll(&mut self, _waker: &mut Waker) -> Result<Poll<()>, Error> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Ok(Poll::Pending)
    }
}
