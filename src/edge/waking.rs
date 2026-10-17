//! An edge that wakes its consumer: [`sync`] plus one waker, cloned into every sender.

use crate::edge::sync;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{
    BoxAs, Closeable, Get, Inlet, Outlet, Pushable, Receivable, Schedule, TryPush, TryPushable,
};
use crate::message::Message;
use crate::signal::Origin;
use crate::wake;
use core::task::Waker;
use std::num::NonZeroUsize;

/// Wakes the consumer when dropped; declared after the inner sender so the edge closes first.
struct WakeOnDrop(Waker);

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.wake_by_ref();
    }
}

/// Producer handle. `Send + Sync`. Wakes the consumer after each landed message, on close and
/// on drop.
pub struct Sender<DataType, SignalType: Origin> {
    inner: sync::Sender<DataType, SignalType>,
    waker: WakeOnDrop,
}

/// Consumer handle. `Send + !Sync + !Clone`. Closing it wakes producers, not its own waker.
///
/// ```compile_fail,E0277
/// fn require_clone<T: Clone>() {}
/// require_clone::<areamy::edge::waking::Receiver<usize, areamy::Trackable<&'static str>>>();
/// ```
///
/// ```compile_fail,E0277
/// fn require_sync<T: Sync>() {}
/// require_sync::<areamy::edge::waking::Receiver<usize, areamy::Trackable<&'static str>>>();
/// ```
pub struct Receiver<DataType, SignalType: Origin> {
    inner: sync::Receiver<DataType, SignalType>,
    /// The consumer's waker, cloned into every sender.
    waker: Waker,
}

impl<DataType, SignalType> Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    /// `bound` caps the buffer, signals included; `None` is unbounded.
    pub(crate) fn new(waker: Waker, bound: Option<NonZeroUsize>) -> Self {
        let inner = match bound {
            Some(bound) => sync::Receiver::bounded(bound),
            None => sync::Receiver::new(),
        };
        Self { inner, waker }
    }

    pub fn sender(&self) -> Sender<DataType, SignalType> {
        Sender {
            inner: self.inner.sender(),
            waker: WakeOnDrop(self.waker.clone()),
        }
    }

    /// Non-blocking pop: `Ok(None)` when empty with a live producer, `Err(Closed)` when empty and
    /// closed or with no producer left. Senders are minted before the consumer runs, so one
    /// nobody writes to is done from the start.
    pub fn poll(&self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        self.inner.try_pop()
    }

    pub fn close(&self) -> Result<(), Error> {
        self.inner.close()
    }

    pub(crate) fn state(&self) -> Result<sync::State, Error> {
        self.inner.state()
    }
}

impl<DataType, SignalType> Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    /// Blocks while a bounded edge is full. Wakes only after the message lands, so the woken
    /// consumer finds it.
    pub fn push_back(&self, message: Message<DataType, SignalType>) -> Result<(), Error> {
        self.inner.push_back(message)?;
        self.waker.0.wake_by_ref();
        Ok(())
    }

    /// This producer is done; the edge closes once every producer is. Idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
        self.inner.close()?;
        self.waker.0.wake_by_ref();
        Ok(())
    }
}

impl<DataType, SignalType> Connection for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
}

impl<DataType, SignalType> Connection for Receiver<DataType, SignalType>
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
    /// Wakes the consumer only when the inner edge took the message.
    fn try_push(
        &mut self,
        message: Message<DataType, SignalType>,
    ) -> Result<TryPush<Message<DataType, SignalType>>, Error> {
        let result = TryPushable::try_push(&mut self.inner, message)?;
        if matches!(result, TryPush::Pushed) {
            self.waker.0.wake_by_ref();
        }
        Ok(result)
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

/// The inner edge's: woken at the next pop or close after a `Full`.
impl<DataType, SignalType> Schedule for Sender<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn schedule(&mut self, waker: wake::Waker) {
        Schedule::schedule(&mut self.inner, waker);
    }
}

impl<DataType, SignalType> Inlet for Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;
}

impl<DataType, SignalType> Receivable for Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn try_recv(&mut self) -> Result<Option<Message<DataType, SignalType>>, Error> {
        self.poll()
    }
}

impl<ConnectionType, DataType, SignalType> Get<ConnectionType> for Receiver<DataType, SignalType>
where
    ConnectionType: Connection + ?Sized,
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    Sender<DataType, SignalType>: BoxAs<ConnectionType>,
{
    fn get(&self) -> Result<Box<ConnectionType>, Error> {
        Ok(self.sender().box_as())
    }
}

#[cfg(test)]
mod tests;
