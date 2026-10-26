//! Wall-clock sleep for async routine bodies.
//!
//! [sleep] needs no handle: the poll loop publishes the current
//! node's timer (`poll::thread::tls`) and the future captures it on
//! first poll — a sleep belongs to the node that first polls it,
//! which is the only node that will ever poll it again.

use crate::error::Error;
use crate::poll::limit::deadline_after;
use crate::poll::thread::tls;
use crate::poll::timer::TimerGuard;

use core::future::Future;
use core::pin::Pin;
use core::task::Poll;
use core::time::Duration;
use std::time::Instant;

/// Sleep for `duration`.
///
/// A *pending* sleep resolves to `Err` if polled outside an areamy
/// poll thread — `poll::sleep(d).await?` propagates it like any
/// routine error. An already-expired sleep resolves `Ok` anywhere
/// (the clock check needs no waker). Huge durations (e.g.
/// `Duration::MAX` as "never") saturate to a far-future deadline
/// instead of panicking on `Instant` overflow. The deadline is fixed
/// at call time.
pub fn sleep(duration: Duration) -> SleepFut {
    SleepFut {
        deadline: deadline_after(duration),
        timer: None,
    }
}

/// Sleep until `deadline`. Absolute-deadline variant of [sleep] —
/// use for drift-free cadences (`next += period; sleep_until(next)`).
/// Same contract as [sleep], resolving immediately if `deadline` has
/// passed.
pub fn sleep_until(deadline: Instant) -> SleepFut {
    SleepFut {
        deadline,
        timer: None,
    }
}

/// Future returned by [sleep]. Resolves once its deadline has passed.
///
/// The armed [TimerGuard] owns the timer captured at first poll and
/// cancels on early drop (lost `Select` race, cancelled routine) so
/// no dead deadline lingers in the heap.
pub struct SleepFut {
    deadline: Instant,
    /// Armed timer, capturing the owning node's timer at first
    /// pending poll — the sleep is frozen to that node.
    timer: Option<TimerGuard>,
}

impl Future for SleepFut {
    type Output = Result<(), Error>;

    /// Ignores `cx` — the wake arrives as a node re-poll via the
    /// deadline heap, not through the std waker.
    fn poll(self: Pin<&mut Self>, _cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if Instant::now() >= this.deadline {
            // Guard drop cancels; a no-op for a fired key, real work
            // only when the clock beats the timer.
            this.timer = None;
            return Poll::Ready(Ok(()));
        }

        match &this.timer {
            Some(_guard) => {
                // A pending re-poll under a different frame's timer
                // means the future migrated nodes — its wake would
                // never reach this frame, hanging the await. The
                // misuse is deterministic, so debug builds catch it;
                // release pays nothing. A TLS-less (manual) re-poll
                // can't be judged.
                #[cfg(debug_assertions)]
                if tls::current_same_target(_guard.timer()) == Some(false) {
                    return Poll::Ready(Err(crate::fatal!(
                        "poll::sleep(..) moved across nodes after first poll"
                    )));
                }
            }
            None => {
                let Some(current) = tls::current() else {
                    return Poll::Ready(Err(crate::fatal!(
                        "poll::sleep(..) awaited outside an areamy poll thread"
                    )));
                };
                this.timer = Some(current.arm(this.deadline));
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poll::scheduler::Scheduler;
    use crate::poll::thread::tls::ThreadLocalGuard;
    use crate::poll::timer::Timer;
    use crate::poll::timer::mock::noop_timer;
    use crate::tests::support::{DEADLINE, deadline};
    use core::task::Context;
    use std::thread;

    fn poll_once(fut: &mut SleepFut) -> Poll<Result<(), Error>> {
        let waker = core::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        Pin::new(fut).poll(&mut cx)
    }

    #[test]
    fn zero_sleep_is_ready_immediately() {
        // Deadline == now at construction; no TLS needed on the
        // expired path.
        let mut fut = sleep(Duration::ZERO);
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok(()))));
    }

    #[test]
    fn pending_sleep_outside_runtime_errors() {
        let mut fut = sleep(Duration::from_secs(60));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Err(_))));
    }

    #[test]
    fn mock_timer_arms_real_timer() {
        // A mock timer has a heap of its own: a sleep under a mock
        // frame arms it and parks.
        let mut fut = sleep(Duration::from_secs(60));
        let _guard = ThreadLocalGuard::set(noop_timer());
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
    }

    #[test]
    fn sleep_arms_and_wakes_owning_node() {
        let _deadline = deadline(DEADLINE);
        let (mut consumer, local, _, _) = Scheduler::started(8);
        let timer = local.context(7).timer;

        let mut fut = sleep(Duration::from_millis(20));
        {
            let _guard = ThreadLocalGuard::set(timer);
            assert!(matches!(poll_once(&mut fut), Poll::Pending));
        }

        // The deadline wakes node 7 (the frame that first polled),
        // and the woken poll resolves.
        let start = Instant::now();
        assert_eq!(consumer.next(), 7);
        assert!(start.elapsed() >= Duration::from_millis(15));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok(()))));
    }

    #[test]
    fn dropped_sleep_cancels_its_deadline() {
        // Initialized: before init a fired timer would push nothing either.
        let (mut consumer, local, _, _) = Scheduler::started(8);
        let timer = local.context(7).timer;

        let mut fut = sleep(Duration::from_millis(20));
        {
            let _guard = ThreadLocalGuard::set(timer);
            assert!(matches!(poll_once(&mut fut), Poll::Pending));
        }
        drop(fut);

        // Past the (cancelled) deadline: without the drop-cancel, node 7
        // would be woken.
        thread::sleep(Duration::from_millis(40));
        assert!(consumer.woken().is_empty());
    }

    #[test]
    fn nested_guard_shadows_and_restores() {
        // Donated-thread nesting (Thread::run inside a poll frame):
        // the inner guard shadows the outer timer only for its own
        // frame; sleeps capture whichever frame first polls them.
        let (_uninit, local) = Scheduler::new().local();
        let outer = local.context(1).timer;
        let inner = local.context(2).timer;

        let mut before = sleep(Duration::from_secs(60));
        let mut nested = sleep(Duration::from_secs(60));
        let mut after = sleep(Duration::from_secs(60));

        let _outer_guard = ThreadLocalGuard::set(outer.clone());
        assert!(matches!(poll_once(&mut before), Poll::Pending));
        {
            let _inner_guard = ThreadLocalGuard::set(inner.clone());
            assert!(matches!(poll_once(&mut nested), Poll::Pending));
        }
        // Inner frame exited — outer timer restored.
        assert!(matches!(poll_once(&mut after), Poll::Pending));

        let armed_on = |fut: &SleepFut, timer: &Timer| {
            fut.timer
                .as_ref()
                .is_some_and(|guard| guard.timer().same_target(timer))
        };
        assert!(armed_on(&before, &outer));
        assert!(armed_on(&nested, &inner));
        assert!(armed_on(&after, &outer));
    }

    #[test]
    fn same_frame_repoll_stays_pending() {
        let (_uninit, local) = Scheduler::new().local();
        let timer = local.context(1).timer;

        let mut fut = sleep(Duration::from_secs(60));
        // Spurious re-polls under the owning frame are the normal
        // case (any wake re-polls the whole future tree) — no false
        // moved-sleep error.
        let _guard = ThreadLocalGuard::set(timer);
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn moved_sleep_errors_under_foreign_frame() {
        let (_uninit, local) = Scheduler::new().local();
        let first = local.context(1).timer;
        let second = local.context(2).timer;

        let mut fut = sleep(Duration::from_secs(60));
        {
            let _guard = ThreadLocalGuard::set(first);
            assert!(matches!(poll_once(&mut fut), Poll::Pending));
        }
        // Pending re-poll under another node's frame: the timer would
        // only ever wake node 1, so node 2's await could never
        // resolve — surfaced as an error, not a silent hang.
        let _guard = ThreadLocalGuard::set(second);
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Err(_))));
    }
}
