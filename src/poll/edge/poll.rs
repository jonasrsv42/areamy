//! A same-thread async edge: one producer, one consumer, both on one [crate::poll::Thread].
//!
//! Optionally bounded. A push wakes the consumer; a pop after a refusal wakes the producer's
//! scheduled waker. Dropping either end closes the edge and wakes the other.

mod receiver;
mod sender;

pub use receiver::Receiver;
pub use sender::Sender;

use crate::message::Message;
use crate::signal::Origin;
use crate::wake::LocalWaker;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::rc::Rc;

/// The state both ends share. Only the buffer is a `RefCell`, never borrowed across a wake.
struct Shared<DataType, SignalType: Origin> {
    buffer: RefCell<VecDeque<Message<DataType, SignalType>>>,
    /// Maximum messages the buffer holds, signals included; `None` is unbounded.
    bound: Option<NonZeroUsize>,
    closed: Cell<bool>,
    /// Refused since the last pop: a retry before then is `Stuck`, and the next pop wakes the
    /// producer.
    refused: Cell<bool>,
    consumer: LocalWaker,
    /// The producer's scheduled waker, read when it fires.
    producer: Cell<Option<LocalWaker>>,
}

impl<DataType, SignalType: Origin> Shared<DataType, SignalType> {
    fn is_full(&self, buffer: &VecDeque<Message<DataType, SignalType>>) -> bool {
        self.bound.is_some_and(|bound| buffer.len() >= bound.get())
    }

    /// Wake the producer, if scheduled, leaving it scheduled. Put back before waking, so a wake
    /// that re-schedules isn't overwritten.
    fn wake_producer(&self) {
        let producer = self.producer.take();
        let waker = producer.clone();
        self.producer.set(producer);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// An edge whose pushes wake `consumer`. `bound` caps the buffer, signals included; `None` is
/// unbounded.
pub(crate) fn channel<DataType, SignalType: Origin>(
    consumer: LocalWaker,
    bound: Option<NonZeroUsize>,
) -> (Sender<DataType, SignalType>, Receiver<DataType, SignalType>) {
    let shared = Rc::new(Shared {
        buffer: RefCell::new(VecDeque::new()),
        bound,
        closed: Cell::new(false),
        refused: Cell::new(false),
        consumer,
        producer: Cell::new(None),
    });
    (Sender::new(shared.clone()), Receiver::new(shared))
}

#[cfg(test)]
mod tests;
