//! Output queue of a [FutureRoutine](super::line::FutureRoutine): the future sends, the node
//! pops. `!Send`, same thread.
//!
//! Optionally bounded. [Sender::send] waits while full; a pop wakes every waiting send. A send
//! wakes the node's Output only after Output's last pop found the queue empty.

mod receiver;
mod sender;
mod shared;
#[cfg(test)]
mod tests;

pub use receiver::Receiver;
pub use sender::Sender;

use crate::poll::timer::Timer;
use crate::wake::LocalWaker;
use shared::Shared;
use std::num::NonZeroUsize;
use std::rc::Rc;

/// A queue holding at most `bound` items (`None`: unbounded); `output` is the node's Output waker.
pub fn channel<T>(
    output: LocalWaker,
    timer: Timer,
    bound: Option<NonZeroUsize>,
) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(Shared::new(output, timer, bound));
    (Sender(shared.clone()), Receiver(shared))
}
