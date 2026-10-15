//! Edges for multi-input nodes: one waiter blocks on all of them at once.

use crate::closed;
use crate::edge::sync;
pub use crate::edge::sync::State;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Get, Outlet, Pushable, TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Origin;
use crate::wake::Parker;
use crate::work::Sink;
use core::task::Waker;
use std::num::NonZeroUsize;

/// Block until any edge is [State::Ready], parking on `parker`, whose waker every edge's
/// senders hold. `Closed` once every edge is [State::Exhausted].
pub(crate) fn wait_any(parker: &Parker, edges: &[&dyn Awaitable]) -> Result<(), Error> {
    loop {
        let mut idle = false;
        for edge in edges {
            match edge.state()? {
                State::Ready => return Ok(()),
                State::Idle => idle = true,
                State::Exhausted => {}
            }
        }
        if !idle {
            return Err(closed!());
        }
        parker.park();
    }
}

/// An edge [wait_any] can wait on. One lock, one readout.
pub(crate) trait Awaitable {
    fn state(&self) -> Result<State, Error>;
}

/// Wakes the group when dropped; declared after the inner sender so the edge closes first.
struct WakeOnDrop(Waker);

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.wake_by_ref();
    }
}

pub struct Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    inner: sync::Sender<D, S>,
    waker: WakeOnDrop,
}

pub struct Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    inner: sync::Receiver<D, S>,
    /// The group's waker, cloned into every sender.
    waker: Waker,
}

impl<D, S> Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// `bound` caps this side alone; each side of a group is bounded independently.
    pub(crate) fn new(waker: Waker, bound: Option<NonZeroUsize>) -> Self {
        let inner = match bound {
            Some(bound) => sync::Receiver::bounded(bound),
            None => sync::Receiver::new(),
        };
        Self { inner, waker }
    }

    pub fn sender(&self) -> Sender<D, S> {
        Sender {
            inner: self.inner.sender(),
            waker: WakeOnDrop(self.waker.clone()),
        }
    }

    pub fn poll(&self) -> Result<Option<Message<D, S>>, Error> {
        self.inner.poll()
    }

    pub fn close(&self) -> Result<(), Error> {
        self.inner.close()
    }
}

impl<D, S> Awaitable for Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn state(&self) -> Result<State, Error> {
        self.inner.state()
    }
}

impl<D, S> Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// Blocks while a bounded side is full. Wakes only after the message lands, so the woken
    /// waiter finds it.
    pub fn push_back(&self, message: Message<D, S>) -> Result<(), Error> {
        self.inner.push_back(message)?;
        self.waker.0.wake_by_ref();
        Ok(())
    }

    pub fn close(&self) -> Result<(), Error> {
        self.inner.close()?;
        self.waker.0.wake_by_ref();
        Ok(())
    }
}

impl<D, S> Connection for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
}

impl<D, S> Connection for Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
}

impl<D, S> Outlet for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    type DataType = D;
    type SignalType = S;
}

impl<D, S> TryPushable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    /// Wakes the group only when the inner edge took the message.
    fn try_push(&mut self, message: Message<D, S>) -> Result<TryPush<Message<D, S>>, Error> {
        let result = TryPushable::try_push(&mut self.inner, message)?;
        if matches!(result, TryPush::Pushed) {
            self.waker.0.wake_by_ref();
        }
        Ok(result)
    }
}

impl<D, S> Pushable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn push(&mut self, message: Message<D, S>) -> Result<(), Error> {
        self.push_back(message)
    }
}

impl<D, S> Closeable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    fn close(&mut self) -> Result<(), Error> {
        Sender::close(self)
    }
}

impl<'params, D, S> Get<dyn Pushable<DataType = D, SignalType = S> + 'params> for Receiver<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = D, SignalType = S> + 'params>, Error> {
        Ok(Box::new(self.sender()))
    }
}

impl<'params, D, S> Get<dyn Sink<DataType = D, SignalType = S> + Send + Sync + 'params>
    for Receiver<D, S>
where
    D: Send + Sync + 'static,
    S: Origin + Send + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Sink<DataType = D, SignalType = S> + Send + Sync + 'params>, Error> {
        Ok(Box::new(self.sender()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::sync::tests::{join_within, wait_until};
    use crate::error::ErrorKind;
    use std::thread;

    type Signal = &'static str;

    type Group = (Parker, Receiver<usize, Signal>, Receiver<usize, Signal>);

    fn pair() -> Group {
        group(None)
    }

    fn bounded_pair(bound: usize) -> Group {
        group(NonZeroUsize::new(bound))
    }

    fn group(bound: Option<NonZeroUsize>) -> Group {
        let parker = Parker::new();
        let left = Receiver::new(parker.waker(), bound);
        let right = Receiver::new(parker.waker(), bound);
        (parker, left, right)
    }

    #[test]
    fn try_push_wakes_only_when_pushed() {
        let (parker, left, _right) = bounded_pair(1);
        let mut tx = left.sender();
        assert_eq!(tx.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert!(parker.take_woken());
        assert_eq!(
            tx.try_push(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
        // Nothing landed, so the consumer must not wake for it.
        assert!(!parker.take_woken());
    }

    #[test]
    fn blocked_push_wakes_after_it_lands() {
        let (parker, left, _right) = bounded_pair(1);
        // Kept alive: dropping the last sender would close the edge.
        let first = left.sender();
        first.push_back(Message::Data(1)).unwrap();
        let tx = left.sender();
        // Hands the sender back: its drop wakes too, which would hide a missing wake.
        let producer = thread::spawn(move || tx.push_back(Message::Data(2)).map(|()| tx));

        wait_until(|| left.inner.producers_waiting() == 1);
        // Forget wakes from before the block; only the landing may wake now.
        parker.take_woken();
        assert_eq!(left.poll().unwrap(), Some(Message::Data(1)));
        let _tx = join_within(producer).unwrap();
        assert!(parker.take_woken());
        assert_eq!(left.poll().unwrap(), Some(Message::Data(2)));
    }

    #[test]
    fn wait_any_returns_when_left_has_data() -> Result<(), Error> {
        let (parker, left, right) = pair();
        let tx = left.sender();
        let _keep = right.sender();
        tx.push_back(Message::Data(1))?;
        wait_any(&parker, &[&left, &right])?;
        assert_eq!(left.poll()?, Some(Message::Data(1)));
        Ok(())
    }

    #[test]
    fn wait_any_wakes_on_right_from_other_thread() -> Result<(), Error> {
        let (parker, left, right) = pair();
        let _keep = left.sender();
        let tx = right.sender();
        thread::scope(|s| -> Result<(), Error> {
            s.spawn(move || tx.push_back(Message::Data(2)));
            wait_any(&parker, &[&left, &right])?;
            assert_eq!(right.poll()?, Some(Message::Data(2)));
            Ok(())
        })
    }

    #[test]
    fn wait_any_closed_without_producers() {
        let (parker, left, right) = pair();
        let result = wait_any(&parker, &[&left, &right]);
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn wait_any_closed_after_last_sender_drops() -> Result<(), Error> {
        let (parker, left, right) = pair();
        let left_tx = left.sender();
        let right_tx = right.sender();
        thread::scope(|s| -> Result<(), Error> {
            s.spawn(move || {
                drop(left_tx);
                drop(right_tx);
            });
            let result = wait_any(&parker, &[&left, &right]);
            assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
            Ok(())
        })
    }

    #[test]
    fn wait_any_keeps_waiting_while_one_side_is_live() -> Result<(), Error> {
        let (parker, left, right) = pair();
        let left_tx = left.sender();
        let right_tx = right.sender();
        drop(left_tx);
        thread::scope(|s| -> Result<(), Error> {
            s.spawn(move || right_tx.push_back(Message::Flush("f")));
            wait_any(&parker, &[&left, &right])?;
            assert_eq!(right.poll()?, Some(Message::Flush("f")));
            Ok(())
        })
    }
}
