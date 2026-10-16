//! Input queue of a [FutureRoutine](super::line::FutureRoutine): the node sends in, the future
//! receives. `!Send`, same thread.
//!
//! Optionally bounded. A refused [Sender::try_send] owes the node's Input one wake, paid by the
//! future's next pop. A send wakes a parked [Receiver::recv] only.

mod receiver;
mod sender;
mod shared;
#[cfg(test)]
mod tests;

pub use receiver::Receiver;
pub use sender::Sender;

use crate::poll::future::waiters::Waiters;
use crate::poll::timer::Timer;
use crate::wake::LocalWaker;
use shared::Shared;
use std::num::NonZeroUsize;
use std::rc::Rc;

/// Item received by the future via [Receiver::recv].
pub enum Input<T> {
    Data(T),
    Flush,
}

/// A queue holding at most `bound` data (`None`: unbounded); `input` is the node's Input waker.
pub fn channel<T>(
    input: LocalWaker,
    timer: Timer,
    bound: Option<NonZeroUsize>,
) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(Shared::new(input, timer, bound, Waiters::default()));
    (Sender(shared.clone()), Receiver(shared))
}
