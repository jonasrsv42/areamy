//! The producer end.

use super::Shared;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Outlet, Schedule, TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Origin;
use crate::wake;
use crate::{closed, fatal};
use std::rc::Rc;

/// Producer handle. `!Send`, `!Clone`: the edge's only producer. Dropping it closes the edge.
///
/// [`Schedule`]: after a [`TryPush::Full`], the scheduled waker is woken at the next pop or when
/// the receiver closes; a [`TryPush::Stuck`] is already owed that wake.
///
/// ```compile_fail,E0277
/// fn require_send<T: Send>() {}
/// require_send::<areamy::poll::edge::Sender<usize, areamy::Trackable<&'static str>>>();
/// ```
pub struct Sender<DataType, SignalType: Origin> {
    shared: Rc<Shared<DataType, SignalType>>,
}

impl<DataType, SignalType: Origin> Sender<DataType, SignalType> {
    pub(super) fn new(shared: Rc<Shared<DataType, SignalType>>) -> Self {
        Self { shared }
    }

    /// Mark the edge closed and wake the consumer. Idempotent.
    fn close_and_wake(&self) {
        if !self.shared.closed.replace(true) {
            self.shared.consumer.wake();
        }
    }
}

impl<DataType, SignalType: Origin> Drop for Sender<DataType, SignalType> {
    fn drop(&mut self) {
        self.close_and_wake();
    }
}

impl<DataType, SignalType: Origin> Connection for Sender<DataType, SignalType> {}

impl<DataType, SignalType: Origin> Outlet for Sender<DataType, SignalType> {
    type DataType = DataType;
    type SignalType = SignalType;
}

impl<DataType, SignalType: Origin> TryPushable for Sender<DataType, SignalType> {
    /// Never blocks. A full edge hands the message back: [`TryPush::Full`] at the first refusal
    /// since the last pop, [`TryPush::Stuck`] after. `Closed` beats full.
    fn try_push(
        &mut self,
        message: Message<DataType, SignalType>,
    ) -> Result<TryPush<Message<DataType, SignalType>>, Error> {
        if self.shared.closed.get() {
            return Err(closed!());
        }
        {
            let mut buffer = self.shared.buffer.try_borrow_mut().map_err(|e| fatal!(e))?;
            if self.shared.is_full(&buffer) {
                if self.shared.refused.replace(true) {
                    return Ok(TryPush::Stuck(message));
                }
                return Ok(TryPush::Full(message));
            }
            buffer.push_back(message);
        }
        // Woken outside the borrow: the waker may touch the edge.
        self.shared.consumer.wake();
        Ok(TryPush::Pushed)
    }
}

impl<DataType, SignalType: Origin> Closeable for Sender<DataType, SignalType> {
    fn close(&mut self) -> Result<(), Error> {
        self.close_and_wake();
        Ok(())
    }
}

/// Only `.local`: the edge never leaves its thread. Re-scheduling replaces the waker.
impl<DataType, SignalType: Origin> Schedule for Sender<DataType, SignalType> {
    fn schedule(&mut self, waker: wake::Waker) {
        self.shared.producer.set(Some(waker.local));
    }
}
