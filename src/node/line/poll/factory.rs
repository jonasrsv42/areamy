//! Factory for creating poll line routines on the async thread.
//!
//! The factory is [Send] (crosses threads). The routine it produces
//! stays on the async thread and does NOT need to be [Send].
//!
//! [LineRoutineFactory::create] receives a [LineWakers] bundle giving
//! the routine access to all three phase wakers.
//!
//! The `'params` parameter bounds how long the factory (and the routine
//! it produces) may hold borrows captured from the surrounding scope.
//! Defaults to `'static` for fully-owning routines via lifetime inference
//! at every call site that doesn't introduce a borrow.

use crate::connect::waker::ThreadLocalWaker;

/// Phase wakers the framework hands to a line routine at construction.
///
/// - `input` wakes the input phase (drains the input edge).
/// - `work` wakes the work phase (the one that polls the routine).
///   `InputQueue::new(wakers.work)` is what makes deadline-driven
///   re-polls (via `recv_with_timeout` / `schedule_at`) route back to
///   the right place.
/// - `output` wakes the output phase (drains the output queue). Pass
///   to `OutputQueue::new` so `push` nudges Output to drain.
pub struct LineWakers {
    pub input: ThreadLocalWaker,
    pub work: ThreadLocalWaker,
    pub output: ThreadLocalWaker,
}

/// Factory for poll line routines. [Send] — crosses threads.
pub trait LineRoutineFactory<'params>: Send + 'params {
    type Routine: 'params;
    fn create(self, wakers: LineWakers) -> Self::Routine;
}

/// Blanket impl: any `FnOnce(LineWakers) -> R + Send + 'params` is a [LineRoutineFactory].
impl<'params, F, R> LineRoutineFactory<'params> for F
where
    F: FnOnce(LineWakers) -> R + Send + 'params,
    R: 'params,
{
    type Routine = R;
    fn create(self, wakers: LineWakers) -> R {
        self(wakers)
    }
}
