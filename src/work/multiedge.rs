//! Waiting on several edges at once: one waiter blocks on all of them.

use crate::closed;
pub use crate::edge::sync::State;
use crate::edge::waking::Receiver;
use crate::error::Error;
use crate::signal::Origin;
use crate::wake::Parker;

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

impl<DataType, SignalType> Awaitable for Receiver<DataType, SignalType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn state(&self) -> Result<State, Error> {
        Receiver::state(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::message::Message;
    use crate::tests::support::{DEADLINE, deadline};
    use std::thread;

    type Signal = &'static str;

    fn pair() -> (Parker, Receiver<usize, Signal>, Receiver<usize, Signal>) {
        let parker = Parker::new();
        let left = Receiver::new(parker.waker(), None);
        let right = Receiver::new(parker.waker(), None);
        (parker, left, right)
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
        let _deadline = deadline(DEADLINE);
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
        let _deadline = deadline(DEADLINE);
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
        let _deadline = deadline(DEADLINE);
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
