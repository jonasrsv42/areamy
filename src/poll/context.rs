//! [Context] — what the runtime hands to everything it polls.

use crate::poll::timer::Timer;
use crate::wake;

/// The node's wakers plus its [Timer], passed to [Pollable::poll](crate::poll::Pollable::poll)
/// and [routine::Poll::poll](crate::node::routine::Poll::poll).
/// Created on the async thread during graph construction — never crosses threads.
///
/// - `wake`: wakes the node, from any thread (`wake.sync`) or this one (`wake.local`); polls
///   don't follow wake order (see [wake order](crate::poll#wake-order))
/// - `timer`: re-polls the node at a deadline
///
/// A clone wakes the same node: `will_wake` holds between them.
#[derive(Clone)]
pub struct Context {
    pub wake: wake::Waker,
    pub timer: Timer,
}

/// [Context] mocks for tests — this crate's, and downstream crates' via the `testing` feature
/// (enable from `[dev-dependencies]` only).
#[cfg(any(test, feature = "testing"))]
pub mod mock {
    use super::Context;
    use crate::poll::timer::mock::noop_timer;
    use crate::wake::mock::noop_waker;

    /// [Context] whose wakes do nothing and whose timer never fires.
    pub fn noop_context() -> Context {
        Context {
            wake: noop_waker(),
            timer: noop_timer(),
        }
    }
}
