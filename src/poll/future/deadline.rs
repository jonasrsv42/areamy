//! The deadline of a `*_with_timeout` wait on a [FutureRoutine](super::line::FutureRoutine) queue.

use crate::poll::limit::deadline_after;
use crate::poll::timer::{Timer, TimerGuard};
use core::task::Poll;
use std::time::{Duration, Instant};

/// Ends a wait at a fixed instant, re-polling the routine then through its [Timer].
pub(crate) struct Deadline {
    /// Fixed at the call, not the first poll; saturates (see [deadline_after]).
    at: Instant,
    /// Armed by the first pending poll; dropping it cancels the timer.
    armed: Option<TimerGuard>,
}

impl Deadline {
    pub(crate) fn after(timeout: Duration) -> Self {
        Self {
            at: deadline_after(timeout),
            armed: None,
        }
    }

    /// Wraps one poll of the wait: `Some` once it resolves, `None` once `at` passes first. A
    /// resolved wait beats an expired deadline.
    pub(crate) fn poll<R>(&mut self, wait: Poll<R>, timer: &Timer) -> Poll<Option<R>> {
        let poll = match wait {
            Poll::Ready(result) => Poll::Ready(Some(result)),
            Poll::Pending if Instant::now() >= self.at => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        };
        match &poll {
            // Resolving releases the timer at once; a fired key cancels as a no-op.
            Poll::Ready(_) => self.armed = None,
            Poll::Pending => {
                if self.armed.is_none() {
                    self.armed = Some(TimerGuard::arm(timer.clone(), self.at));
                }
            }
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poll::queue::PollQueue;

    fn timer() -> Timer {
        // A real Timer bound to a real Scheduler; the consumer never runs here.
        let pq = PollQueue::new();
        let (_consumer, local_producer) = pq.local();
        Timer::new(0, &local_producer)
    }

    fn expired() -> Deadline {
        Deadline {
            at: Instant::now() - Duration::from_millis(50),
            armed: None,
        }
    }

    #[test]
    fn duration_max_saturates() {
        // Duration::MAX as "never" must not panic on Instant overflow.
        let mut deadline = Deadline::after(Duration::MAX);
        assert!(deadline.poll(Poll::<()>::Pending, &timer()).is_pending());
    }

    #[test]
    fn a_pending_wait_arms_the_timer_once() {
        let timer = timer();
        let mut deadline = Deadline::after(Duration::from_secs(60));
        assert!(deadline.poll(Poll::<()>::Pending, &timer).is_pending());
        let key = deadline.armed.as_ref().map(TimerGuard::key);
        assert!(key.is_some());
        assert!(deadline.poll(Poll::<()>::Pending, &timer).is_pending());
        assert_eq!(deadline.armed.as_ref().map(TimerGuard::key), key);
    }

    #[test]
    fn resolving_releases_the_timer() {
        let timer = timer();
        let mut deadline = Deadline::after(Duration::from_secs(60));
        assert!(deadline.poll(Poll::<()>::Pending, &timer).is_pending());
        assert_eq!(deadline.poll(Poll::Ready(7), &timer), Poll::Ready(Some(7)));
        assert!(deadline.armed.is_none());
    }

    #[test]
    fn an_expired_deadline_ends_a_pending_wait() {
        let mut deadline = expired();
        assert_eq!(
            deadline.poll(Poll::<()>::Pending, &timer()),
            Poll::Ready(None)
        );
        assert!(deadline.armed.is_none());
    }

    #[test]
    fn a_resolved_wait_beats_an_expired_deadline() {
        let mut deadline = expired();
        assert_eq!(
            deadline.poll(Poll::Ready(9), &timer()),
            Poll::Ready(Some(9))
        );
    }
}
