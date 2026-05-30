//! Factory for creating poll biunion routines on the async thread.
//!
//! Same pattern as [line's factory](crate::node::line::poll::factory) —
//! the factory is [Send] (crosses threads), the routine it produces
//! stays on the async thread.
//!
//! The `'params` parameter bounds how long the factory (and the routine
//! it produces) may hold borrows captured from the surrounding scope.

use crate::connect::waker::ThreadLocalWaker;

/// Per-input wakers, grouped under [`BiunionWakers::input`] so a
/// routine accesses them via `wakers.input.left` / `wakers.input.right`.
pub struct BiunionInputs {
    pub left: ThreadLocalWaker,
    pub right: ThreadLocalWaker,
}

/// Phase wakers the framework hands to a biunion routine.
///
/// - `input.left` / `input.right` wake the two input phases.
/// - `work` wakes the work phase (polls the routine). Used by
///   `recv_with_timeout` on either input so the routine re-polls at
///   the deadline.
/// - `output` wakes the output phase to drain.
pub struct BiunionWakers {
    pub input: BiunionInputs,
    pub work: ThreadLocalWaker,
    pub output: ThreadLocalWaker,
}

/// Factory for poll biunion routines. [Send] — crosses threads.
pub trait BiunionRoutineFactory<'params>: Send + 'params {
    type Routine: 'params;
    fn create(self, wakers: BiunionWakers) -> Self::Routine;
}

/// Blanket impl: any `FnOnce(BiunionWakers) -> R + Send + 'params` is a [BiunionRoutineFactory].
impl<'params, F, R> BiunionRoutineFactory<'params> for F
where
    F: FnOnce(BiunionWakers) -> R + Send + 'params,
    R: 'params,
{
    type Routine = R;
    fn create(self, wakers: BiunionWakers) -> R {
        self(wakers)
    }
}
