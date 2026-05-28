//! Factory for creating poll line routines on the async thread.
//!
//! The factory is [Send] (crosses threads). The routine it produces
//! stays on the async thread and does NOT need to be [Send].
//!
//! [LineRoutineFactory::create] receives a [ThreadLocalWaker] for the output
//! phase so the routine can wake Output when it produces data.
//!
//! The `'params` parameter bounds how long the factory (and the routine
//! it produces) may hold borrows captured from the surrounding scope.
//! Defaults to `'static` for fully-owning routines via lifetime inference
//! at every call site that doesn't introduce a borrow.

use crate::connect::waker::ThreadLocalWaker;

/// Factory for poll line routines. [Send] — crosses threads.
///
/// [LineRoutineFactory::create] is called on the async thread with
/// the output phase waker. The routine stores it and wakes Output
/// when it sends data, avoiding unnecessary output polls.
pub trait LineRoutineFactory<'params>: Send + 'params {
    type Routine: 'params;
    fn create(self, output_waker: ThreadLocalWaker) -> Self::Routine;
}

/// Blanket impl: any `FnOnce(ThreadLocalWaker) -> R + Send + 'params` is a [LineRoutineFactory].
impl<'params, F, R> LineRoutineFactory<'params> for F
where
    F: FnOnce(ThreadLocalWaker) -> R + Send + 'params,
    R: 'params,
{
    type Routine = R;
    fn create(self, output_waker: ThreadLocalWaker) -> R {
        self(output_waker)
    }
}
