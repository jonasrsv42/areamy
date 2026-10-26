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

use crate::poll::Context;
use crate::wake::LocalWaker;

/// What the framework hands a line routine at construction: its I/O phases' wakers and its own
/// context.
///
/// - `input` wakes the input phase (drains the input edge). Wake it after
///   [crate::node::routine::Accept::Full] once room frees.
/// - `work` is the work phase's context (the phase that polls the routine): the very one the
///   routine is polled with, so its wakers `will_wake` the `cx` a future sees, and its timer
///   re-polls the routine at a deadline.
/// - `output` wakes the output phase (drains the routine's output).
///
/// Polls don't follow wake order: see [wake order](crate::poll#wake-order).
pub struct LineWakers {
    pub input: LocalWaker,
    pub work: Context,
    pub output: LocalWaker,
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
