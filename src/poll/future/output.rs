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

use crate::poll::Context;
use crate::wake::LocalWaker;
use alloc::rc::Rc;
use core::num::NonZeroUsize;
use shared::Shared;

/// A queue holding at most `bound` items (`None`: unbounded). `output` is the node's Output
/// waker, `home` the context the routine is polled with (`LineWakers::work`).
pub fn channel<T>(
    output: LocalWaker,
    home: Context,
    bound: Option<NonZeroUsize>,
) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(Shared::new(output, home.wake, home.timer, bound));
    (Sender(shared.clone()), Receiver(shared))
}
