//! Wall-clock sleep for async routine bodies.
//!
//! [sleep] needs no handle: the poll loop publishes the current
//! node's waker (`thread::poll::tls`) and the future captures it on
//! first poll — a sleep belongs to the node that first polls it,
//! which is the only node that will ever poll it again.

use crate::connect::poll::limit::deadline_after;
use crate::connect::poll::wakers::TimerGuard;
use crate::error::Error;
use crate::thread::poll::tls;

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

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

/// Future returned by [sleep]. Resolves once its deadline has passed.
///
/// The armed [TimerGuard] owns the waker captured at first poll and
/// cancels on early drop (lost `Select` race, cancelled routine) so
/// no dead deadline lingers in the heap.
pub struct SleepFut {
    deadline: Instant,
    /// Armed timer, capturing the owning node's waker at first
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
                // A pending re-poll under a different frame's waker
                // means the future migrated nodes — its wake would
                // never reach this frame, hanging the await. The
                // misuse is deterministic, so debug builds catch it;
                // release pays nothing. A TLS-less (manual) re-poll
                // can't be judged.
                #[cfg(debug_assertions)]
                if tls::current_will_wake(_guard.waker()) == Some(false) {
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
                this.timer = Some(TimerGuard::arm(&current, this.deadline));
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::poll::queue::PollQueue;
    use crate::connect::waker::{ThreadLocalWaker, mock};
    use crate::thread::poll::tls::ThreadLocalGuard;
    use std::task::Context;
    use std::thread;

    fn poll_once(fut: &mut SleepFut) -> Poll<Result<(), Error>> {
        let waker = std::task::Waker::noop();
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
    fn mock_waker_arms_real_timer() {
        // Mocks carry a private scheduler, so schedule_at is
        // infallible everywhere — a sleep under a mock frame parks
        // normally instead of erroring.
        let mut fut = sleep(Duration::from_secs(60));
        let _guard = ThreadLocalGuard::set(mock::noop_local_waker());
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
    }

    #[test]
    fn sleep_arms_and_wakes_owning_node() {
        let q = PollQueue::new();
        let (mut consumer, local) = q.local();
        let waker = ThreadLocalWaker::from_producer(7, &local);

        let mut fut = sleep(Duration::from_millis(20));
        {
            let _guard = ThreadLocalGuard::set(waker);
            assert!(matches!(poll_once(&mut fut), Poll::Pending));
        }

        // The deadline wakes node 7 (the frame that first polled),
        // and the woken poll resolves.
        let start = Instant::now();
        assert_eq!(consumer.next().unwrap(), 7);
        assert!(start.elapsed() >= Duration::from_millis(15));
        assert!(matches!(poll_once(&mut fut), Poll::Ready(Ok(()))));
    }

    #[test]
    fn dropped_sleep_cancels_its_deadline() {
        let q = PollQueue::new();
        let (mut consumer, local) = q.local();
        let waker = ThreadLocalWaker::from_producer(7, &local);

        let mut fut = sleep(Duration::from_millis(20));
        {
            let _guard = ThreadLocalGuard::set(waker);
            assert!(matches!(poll_once(&mut fut), Poll::Pending));
        }
        drop(fut);

        // Let the (cancelled) deadline expire, then push. Without the
        // drop-cancel, next() would surface node 7's expired deadline
        // ahead of the push; with it, the push is the only event.
        thread::sleep(Duration::from_millis(40));
        local.push(9);
        assert_eq!(consumer.next().unwrap(), 9);
    }

    #[test]
    fn nested_guard_shadows_and_restores() {
        // Donated-thread nesting (Thread::run inside a poll frame):
        // the inner guard shadows the outer waker only for its own
        // frame; sleeps capture whichever frame first polls them.
        let q = PollQueue::new();
        let (mut consumer, local) = q.local();
        let outer = ThreadLocalWaker::from_producer(1, &local);
        let inner = ThreadLocalWaker::from_producer(2, &local);

        let mut before = sleep(Duration::from_millis(10));
        let mut nested = sleep(Duration::from_millis(20));
        let mut after = sleep(Duration::from_millis(30));

        let _outer_guard = ThreadLocalGuard::set(outer);
        assert!(matches!(poll_once(&mut before), Poll::Pending));
        {
            let _inner_guard = ThreadLocalGuard::set(inner);
            assert!(matches!(poll_once(&mut nested), Poll::Pending));
        }
        // Inner frame exited — outer waker restored.
        assert!(matches!(poll_once(&mut after), Poll::Pending));

        assert_eq!(consumer.next().unwrap(), 1); // before → outer
        assert_eq!(consumer.next().unwrap(), 2); // nested → inner
        assert_eq!(consumer.next().unwrap(), 1); // after → outer
    }

    #[test]
    fn same_frame_repoll_stays_pending() {
        let q = PollQueue::new();
        let (_consumer, local) = q.local();
        let waker = ThreadLocalWaker::from_producer(1, &local);

        let mut fut = sleep(Duration::from_secs(60));
        // Spurious re-polls under the owning frame are the normal
        // case (any wake re-polls the whole future tree) — no false
        // moved-sleep error.
        let _guard = ThreadLocalGuard::set(waker);
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
        assert!(matches!(poll_once(&mut fut), Poll::Pending));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn moved_sleep_errors_under_foreign_frame() {
        let q = PollQueue::new();
        let (_consumer, local) = q.local();
        let first = ThreadLocalWaker::from_producer(1, &local);
        let second = ThreadLocalWaker::from_producer(2, &local);

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
