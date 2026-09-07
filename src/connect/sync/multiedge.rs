//! Edges for multi-input nodes: one waiter blocks on all of them at once.

use super::edge;
pub use super::edge::State;
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::signal::Origin;
use crate::{Closeable, Pushable, Sink, closed, fatal, graph::Get};
use std::sync::{Arc, Condvar, Mutex};

/// Wake flag shared by a group of edges. Single waiter.
pub(crate) struct Notify {
    flag: Mutex<bool>,
    signal: Condvar,
}

impl Notify {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            flag: Mutex::new(false),
            signal: Condvar::new(),
        })
    }

    fn raise(&self) {
        let mut flag = self.flag.lock().unwrap_or_else(|p| p.into_inner());
        *flag = true;
        self.signal.notify_one();
    }

    /// Block until raised since the last wait.
    fn wait(&self) -> Result<(), Error> {
        let mut flag = self.flag.lock().map_err(|e| fatal!(e))?;
        while !*flag {
            flag = self.signal.wait(flag).map_err(|e| fatal!(e))?;
        }
        *flag = false;
        Ok(())
    }

    /// Block until any edge is [State::Ready]. `Closed` once every edge
    /// is [State::Exhausted].
    pub(crate) fn wait_any(&self, edges: &[&dyn Awaitable]) -> Result<(), Error> {
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
            self.wait()?;
        }
    }
}

/// An edge a [Notify] can wait on. One lock, one readout.
pub(crate) trait Awaitable {
    fn state(&self) -> Result<State, Error>;
}

/// Raises the group flag when dropped; declared after the inner sender
/// so the edge closes first.
struct RaiseOnDrop(Arc<Notify>);

impl Drop for RaiseOnDrop {
    fn drop(&mut self) {
        self.0.raise();
    }
}

pub struct Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    inner: edge::Sender<D, S>,
    raise: RaiseOnDrop,
}

pub struct Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    inner: edge::Receiver<D, S>,
    notify: Arc<Notify>,
}

impl<D, S> Receiver<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    pub(crate) fn new(notify: Arc<Notify>) -> Self {
        Self {
            inner: edge::Receiver::new(),
            notify,
        }
    }

    pub fn sender(&self) -> Sender<D, S> {
        Sender {
            inner: self.inner.sender(),
            raise: RaiseOnDrop(self.notify.clone()),
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
    pub fn push_back(&self, message: Message<D, S>) -> Result<(), Error> {
        self.inner.push_back(message)?;
        self.raise.0.raise();
        Ok(())
    }

    pub fn close(&self) -> Result<(), Error> {
        self.inner.close()?;
        self.raise.0.raise();
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

impl<D, S> Pushable for Sender<D, S>
where
    D: Send + Sync,
    S: Origin + Send + Sync,
{
    type DataType = D;
    type SignalType = S;

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
    use crate::error::ErrorKind;
    use std::thread;

    type Signal = &'static str;

    fn pair() -> (
        Arc<Notify>,
        Receiver<usize, Signal>,
        Receiver<usize, Signal>,
    ) {
        let notify = Notify::new();
        let left = Receiver::new(notify.clone());
        let right = Receiver::new(notify.clone());
        (notify, left, right)
    }

    #[test]
    fn wait_any_returns_when_left_has_data() -> Result<(), Error> {
        let (notify, left, right) = pair();
        let tx = left.sender();
        let _keep = right.sender();
        tx.push_back(Message::Data(1))?;
        notify.wait_any(&[&left, &right])?;
        assert_eq!(left.poll()?, Some(Message::Data(1)));
        Ok(())
    }

    #[test]
    fn wait_any_wakes_on_right_from_other_thread() -> Result<(), Error> {
        let (notify, left, right) = pair();
        let _keep = left.sender();
        let tx = right.sender();
        thread::scope(|s| -> Result<(), Error> {
            s.spawn(move || tx.push_back(Message::Data(2)));
            notify.wait_any(&[&left, &right])?;
            assert_eq!(right.poll()?, Some(Message::Data(2)));
            Ok(())
        })
    }

    #[test]
    fn wait_any_closed_without_producers() {
        let (notify, left, right) = pair();
        let result = notify.wait_any(&[&left, &right]);
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    }

    #[test]
    fn wait_any_closed_after_last_sender_drops() -> Result<(), Error> {
        let (notify, left, right) = pair();
        let left_tx = left.sender();
        let right_tx = right.sender();
        thread::scope(|s| -> Result<(), Error> {
            s.spawn(move || {
                drop(left_tx);
                drop(right_tx);
            });
            let result = notify.wait_any(&[&left, &right]);
            assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
            Ok(())
        })
    }

    #[test]
    fn wait_any_keeps_waiting_while_one_side_is_live() -> Result<(), Error> {
        let (notify, left, right) = pair();
        let left_tx = left.sender();
        let right_tx = right.sender();
        drop(left_tx);
        thread::scope(|s| -> Result<(), Error> {
            s.spawn(move || right_tx.push_back(Message::Flush("f")));
            notify.wait_any(&[&left, &right])?;
            assert_eq!(right.poll()?, Some(Message::Flush("f")));
            Ok(())
        })
    }
}
