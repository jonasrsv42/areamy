//! Factory for creating poll line routines on the async thread.
//!
//! The factory is [Send] (crosses threads). The routine it produces
//! stays on the async thread and does NOT need to be [Send].
//!
//! [PollLineRoutineFactory::create] receives a [ThreadLocalWaker] for the output
//! phase so the routine can wake Output when it produces data.

use crate::connect::waker::ThreadLocalWaker;

/// Factory for poll line routines. [Send] — crosses threads.
///
/// [PollLineRoutineFactory::create] is called on the async thread with
/// the output phase waker. The routine stores it and wakes Output
/// when it sends data, avoiding unnecessary output polls.
pub trait PollLineRoutineFactory: Send {
    type Routine;
    fn create(self, output_waker: ThreadLocalWaker) -> Self::Routine;
}

/// Blanket impl: any `FnOnce(ThreadLocalWaker) -> R + Send` is a [PollLineRoutineFactory].
impl<F, R> PollLineRoutineFactory for F
where
    F: FnOnce(ThreadLocalWaker) -> R + Send,
{
    type Routine = R;
    fn create(self, output_waker: ThreadLocalWaker) -> R {
        self(output_waker)
    }
}
