//! [Single] pushes each message into one edge and holds the one it refused. Never clones.

use crate::edge::fanout::Rotation;
use crate::edge::rotate::Rotate;
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Outlet, Schedule, TryPushable};
use crate::message::Message;
use crate::wake;

/// A fan-out of one: the edge takes the move, so the data type need not be `Clone`.
pub struct Single<EdgeType: Outlet> {
    edge: EdgeType,
    /// The message the edge refused, delivered by the next [Rotate::retry].
    held: Option<Message<EdgeType::DataType, EdgeType::SignalType>>,
}

impl<EdgeType: Outlet> From<EdgeType> for Single<EdgeType> {
    fn from(edge: EdgeType) -> Self {
        Single { edge, held: None }
    }
}

impl<EdgeType: Outlet> Connection for Single<EdgeType> {}

impl<EdgeType: Outlet> Outlet for Single<EdgeType> {
    type DataType = EdgeType::DataType;
    type SignalType = EdgeType::SignalType;
}

impl<EdgeType: Outlet + Schedule> Schedule for Single<EdgeType> {
    fn schedule(&mut self, waker: wake::Waker) {
        self.edge.schedule(waker);
    }
}

/// A held message is dropped.
impl<EdgeType: Outlet + Closeable> Closeable for Single<EdgeType> {
    fn close(&mut self) -> Result<(), Error> {
        self.held = None;
        self.edge.close()
    }
}

impl<EdgeType: TryPushable + Schedule + Closeable> Single<EdgeType> {
    fn deliver(
        &mut self,
        message: Message<EdgeType::DataType, EdgeType::SignalType>,
    ) -> Result<Rotation, Error> {
        match self.edge.try_push(message)?.refused() {
            None => Ok(Rotation::Complete),
            Some(refused) => {
                self.held = Some(refused);
                Ok(Rotation::Suspended(0))
            }
        }
    }
}

impl<EdgeType: TryPushable + Schedule + Closeable> Rotate for Single<EdgeType> {
    fn suspended(&self) -> Option<usize> {
        self.held.as_ref().map(|_| 0)
    }

    fn retry(&mut self) -> Result<Rotation, Error> {
        match self.held.take() {
            None => Ok(Rotation::Complete),
            Some(message) => self.deliver(message),
        }
    }

    fn rotate(
        &mut self,
        message: Message<EdgeType::DataType, EdgeType::SignalType>,
    ) -> Result<Rotation, Error> {
        // Unreachable from a node: it rotates only after `retry` completes.
        if self.held.is_some() {
            return Err(fatal!("output pushed past a held message"));
        }
        self.deliver(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Trackable;
    use crate::edge::sync::{Receiver, Sender};
    use crate::error::ErrorKind;
    use crate::wake::mock::counting_waker;
    use std::num::NonZeroUsize;
    use std::sync::atomic::Ordering;

    type Signal = Trackable<&'static str>;

    /// Not `Clone`: a [Single] must never need one.
    #[derive(Debug, PartialEq)]
    struct Owned(usize);

    fn single(bound: usize) -> (Single<Sender<Owned, Signal>>, Receiver<Owned, Signal>) {
        let receiver = Receiver::bounded(NonZeroUsize::new(bound).unwrap());
        (Single::from(receiver.sender()), receiver)
    }

    #[test]
    fn rotate_delivers_by_move() {
        let (mut single, receiver) = single(1);
        assert_eq!(
            single.rotate(Message::Data(Owned(1))).unwrap(),
            Rotation::Complete
        );
        assert_eq!(single.suspended(), None);
        assert_eq!(receiver.read_front().unwrap(), Message::Data(Owned(1)));
    }

    #[test]
    fn a_refusal_holds_until_retry_delivers() {
        let (mut single, receiver) = single(1);
        assert_eq!(
            single.rotate(Message::Data(Owned(1))).unwrap(),
            Rotation::Complete
        );
        assert_eq!(
            single.rotate(Message::Data(Owned(2))).unwrap(),
            Rotation::Suspended(0)
        );
        assert_eq!(single.suspended(), Some(0));
        // Still full: refused again, still held.
        assert_eq!(single.retry().unwrap(), Rotation::Suspended(0));
        assert_eq!(receiver.read_front().unwrap(), Message::Data(Owned(1)));
        assert_eq!(single.retry().unwrap(), Rotation::Complete);
        assert_eq!(single.suspended(), None);
        assert_eq!(receiver.read_front().unwrap(), Message::Data(Owned(2)));
        // Nothing held: retry has nothing to do.
        assert_eq!(single.retry().unwrap(), Rotation::Complete);
    }

    #[test]
    fn rotate_while_held_errors_and_keeps_the_held_message() {
        let (mut single, receiver) = single(1);
        let _ = single.rotate(Message::Data(Owned(1))).unwrap();
        let _ = single.rotate(Message::Data(Owned(2))).unwrap();
        assert!(single.rotate(Message::Data(Owned(3))).is_err());
        assert_eq!(receiver.read_front().unwrap(), Message::Data(Owned(1)));
        assert_eq!(single.retry().unwrap(), Rotation::Complete);
        assert_eq!(receiver.read_front().unwrap(), Message::Data(Owned(2)));
    }

    #[test]
    fn close_drops_the_held_message_and_closes_the_edge() {
        let (mut single, receiver) = single(1);
        let _ = single.rotate(Message::Data(Owned(1))).unwrap();
        let _ = single.rotate(Message::Data(Owned(2))).unwrap();
        single.close().unwrap();
        assert_eq!(single.suspended(), None);
        assert_eq!(receiver.read_front().unwrap(), Message::Data(Owned(1)));
        assert!(matches!(
            receiver.read_front().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn schedule_binds_the_edge() {
        let (mut single, receiver) = single(1);
        let (waker, wakes) = counting_waker();
        single.schedule(waker);
        let _ = single.rotate(Message::Data(Owned(1))).unwrap();
        let _ = single.rotate(Message::Data(Owned(2))).unwrap();
        // The refusal owes the bound waker a wake at the next pop.
        let _ = receiver.read_front().unwrap();
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
    }
}
