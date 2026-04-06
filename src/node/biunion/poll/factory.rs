//! Factory for creating poll biunion routines on the async thread.
//!
//! Same pattern as [line's factory](crate::node::line::poll::factory) —
//! the factory is [Send] (crosses threads), the routine it produces
//! stays on the async thread.

use crate::connect::waker::ThreadLocalWaker;

/// Factory for poll biunion routines. [Send] — crosses threads.
///
/// [BiunionRoutineFactory::create] is called on the async thread with
/// the output phase waker.
pub trait BiunionRoutineFactory: Send {
    type Routine;
    fn create(self, output_waker: ThreadLocalWaker) -> Self::Routine;
}

/// Blanket impl: any `FnOnce(ThreadLocalWaker) -> R + Send` is a [BiunionRoutineFactory].
impl<F, R> BiunionRoutineFactory for F
where
    F: FnOnce(ThreadLocalWaker) -> R + Send,
{
    type Routine = R;
    fn create(self, output_waker: ThreadLocalWaker) -> R {
        self(output_waker)
    }
}
