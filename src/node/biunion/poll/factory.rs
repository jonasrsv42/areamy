//! Factory for creating poll biunion routines on the async thread.
//!
//! Same pattern as [line's factory](crate::node::line::poll::factory) —
//! the factory is [Send] (crosses threads), the routine it produces
//! stays on the async thread.
//!
//! The `'params` parameter bounds how long the factory (and the routine
//! it produces) may hold borrows captured from the surrounding scope.

use crate::connect::waker::ThreadLocalWaker;

/// Factory for poll biunion routines. [Send] — crosses threads.
///
/// [BiunionRoutineFactory::create] is called on the async thread with
/// the output phase waker.
pub trait BiunionRoutineFactory<'params>: Send + 'params {
    type Routine: 'params;
    fn create(self, output_waker: ThreadLocalWaker) -> Self::Routine;
}

/// Blanket impl: any `FnOnce(ThreadLocalWaker) -> R + Send + 'params` is a [BiunionRoutineFactory].
impl<'params, F, R> BiunionRoutineFactory<'params> for F
where
    F: FnOnce(ThreadLocalWaker) -> R + Send + 'params,
    R: 'params,
{
    type Routine = R;
    fn create(self, output_waker: ThreadLocalWaker) -> R {
        self(output_waker)
    }
}
