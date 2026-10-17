//! The consumer end.

use super::Shared;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Inlet, Receivable};
use crate::message::Message;
use crate::signal::Origin;
use crate::{closed, fatal};
use alloc::rc::Rc;

/// Consumer handle. `!Send`, `!Clone`: the edge's only consumer. Dropping it closes the edge.
///
/// ```compile_fail,E0277
/// fn require_send<T: Send>() {}
/// require_send::<areamy::poll::edge::Receiver<usize, areamy::Trackable<&'static str>>>();
/// ```
pub struct Receiver<DataType, SignalType: Origin> {
    shared: Rc<Shared<DataType, SignalType>>,
}

impl<DataType, SignalType: Origin> Receiver<DataType, SignalType> {
    pub(super) fn new(shared: Rc<Shared<DataType, SignalType>>) -> Self {
        Self { shared }
    }

    /// Mark the edge closed and wake a producer refused since the last pop: its retry finds the
    /// edge closed. Idempotent. A producer not refused since then learns of it only at its next
    /// push.
    pub fn close(&self) {
        self.shared.closed.set(true);
        if self.shared.refused.take() {
            self.shared.wake_producer();
        }
    }
}

impl<DataType, SignalType: Origin> Drop for Receiver<DataType, SignalType> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<DataType, SignalType: Origin> Connection for Receiver<DataType, SignalType> {}

impl<DataType, SignalType: Origin> Inlet for Receiver<DataType, SignalType> {
    type DataType = DataType;
    type SignalType = SignalType;
}

impl<DataType, SignalType: Origin> Receivable for Receiver<DataType, SignalType> {
    /// A pop after a refusal wakes the producer, once.
    fn try_recv(&mut self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        let popped = {
            let mut buffer = self.shared.buffer.try_borrow_mut().map_err(|e| fatal!(e))?;
            buffer.pop_front()
        };
        match popped {
            Some(message) => {
                // Woken outside the borrow: the waker may push.
                if self.shared.refused.take() {
                    self.shared.wake_producer();
                }
                Ok(Some(message))
            }
            None if self.shared.closed.get() => Err(closed!()),
            None => Ok(None),
        }
    }
}
