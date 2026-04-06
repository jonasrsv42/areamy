//! [`LineRoutine`] is the poll/async counterpart of
//! [LineRoutine](crate::LineRoutine) (sync).
//!
//! Prefer [FutureRoutine](crate::poll::FutureRoutine) over raw impls —
//! it enforces all contracts below by construction.
//!
//! Unlike the sync [LineRoutine](crate::LineRoutine), does NOT require
//! [std::marker::Send]. Async routines are created on the async thread via
//! [LineRoutineFactory](super::factory::LineRoutineFactory) and never cross
//! threads. This allows routines to hold non-Send types like `Rc<RefCell<_>>`
//! for zero-cost shared state with their futures.
//!
//! The factory receives a [ThreadLocalWaker](crate::connect::waker::ThreadLocalWaker)
//! for the Output phase. Routines MUST use this to wake Output when they
//! produce data (e.g. via [OutputProducer](crate::poll::future::queue::OutputProducer)).
//!
//! ## [crate::Send] contract
//!
//! If the routine needs async processing to handle data received
//! via [crate::Send], it MUST arrange for [crate::Poll] to be woken
//! (e.g. via a waker stored during a previous [crate::Poll] call).
//! Failing to do so will deadlock — [crate::Poll] is not automatically
//! invoked after [crate::Send].
//!
//! Routines that produce output synchronously in [crate::Send]
//! do not need to wake [crate::Poll].
//!
//! [FutureRoutine](crate::poll::FutureRoutine) handles this via its
//! waker-aware [InputQueue](crate::poll::future::queue::InputQueue) —
//! push wakes Poll automatically.
//!
//! ## [crate::Next] contract
//!
//! The node's Output phase calls [crate::Next] to drain output. Output
//! is NOT polled automatically — the routine MUST wake the Output phase
//! when it produces data. Use [OutputProducer::push](crate::poll::future::queue::OutputProducer::push)
//! which wakes Output via [ThreadLocalWaker](crate::connect::waker::ThreadLocalWaker).
//!
//! ## [crate::Poll] contract
//!
//! [crate::Poll::poll] receives a [Waker](crate::connect::waker::Waker)
//! carrying both a sync waker (for I/O / standard futures) and a
//! thread-local waker (for cheap same-thread wake).
//!
//! - [core::task::Poll::Pending] — async work in progress.
//! - [core::task::Poll::Ready] — routine finished processing after
//!   [crate::Flush].
//!
//! After [crate::Flush], [crate::Poll] is invoked once. If the routine
//! returns Ready, the flush is complete. If it returns Pending, the
//! runtime will NOT wake the routine again — the routine must arrange
//! for itself to be woken (e.g. by handing its waker to an I/O source,
//! timer, or interrupt that will fire). The node will be waiting for
//! a Ready signal to eventually arrive. Failure to wake itself leads
//! to deadlock.
//!
//! Returning Ready outside of a flush is a fatal error.
//!
//! Returning [crate::error::ErrorKind::Closed] is a fatal error.
//!
//! ## TL;DR
//!
//! - Output data? Wake Output via [OutputProducer](crate::poll::future::queue::OutputProducer) or the factory's [ThreadLocalWaker](crate::connect::waker::ThreadLocalWaker). Output is NOT polled automatically.
//! - Need async work after [crate::Send]? Wake Work (e.g. via [InputQueue](crate::poll::future::queue::InputQueue) push).
//! - After [crate::Flush], return [core::task::Poll::Ready] from [crate::Poll] (immediately or eventually). Deadlock otherwise.
//! - Never return [core::task::Poll::Ready] outside flush. Fatal error.
//! - Never return [crate::error::ErrorKind::Closed] from [crate::Poll]. Fatal error — the routine does not manage its own lifecycle.
//!
//! Think this is too many rules? Just use [FutureRoutine](crate::poll::FutureRoutine) — it handles all of it for you.

pub trait LineRoutine<In, Out>:
    crate::Send<In> + crate::Next<Out> + crate::Flush + crate::Poll + crate::node::Name
{
}

#[cfg(test)]
pub mod tests {
    use super::LineRoutine;
    use crate::connect::waker::{ThreadLocalWake, ThreadLocalWaker, Waker};
    use crate::error::Error;
    use crate::poll::future::queue::OutputQueue;
    use crate::{Next, Send};

    pub struct MockLine {
        state: usize,
        output: OutputQueue<usize>,
        pub poll_count: usize,
        flushed: bool,
    }

    impl MockLine {
        pub fn new(output_waker: ThreadLocalWaker) -> Self {
            MockLine {
                state: 0,
                output: OutputQueue::new(output_waker),
                poll_count: 0,
                flushed: false,
            }
        }
    }

    impl crate::Send<usize> for MockLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.state += message;
            self.output.producer.push(self.state * 2);
            Ok(())
        }
    }

    impl crate::Next<usize> for MockLine {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.output.consumer.pop())
        }
    }

    impl crate::Flush for MockLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.state = 0;
            self.flushed = true;
            Ok(())
        }
    }

    impl crate::Poll for MockLine {
        fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
            self.poll_count += 1;
            if self.flushed {
                self.flushed = false;
                return Ok(core::task::Poll::Ready(()));
            }
            Ok(core::task::Poll::Pending)
        }
    }

    impl crate::node::Name for MockLine {}
    impl LineRoutine<usize, usize> for MockLine {}

    struct NoopWake;
    impl ThreadLocalWake for NoopWake {
        fn wake(&self) {}
    }

    fn noop_local_waker() -> ThreadLocalWaker {
        ThreadLocalWaker::new(NoopWake)
    }

    fn noop_waker() -> Waker {
        Waker {
            sync: std::task::Waker::noop().clone(),
            local: ThreadLocalWaker::new(NoopWake),
        }
    }

    #[test]
    fn poll_line_send_next_works() {
        let mut line = MockLine::new(noop_local_waker());
        line.send(2).unwrap();
        assert_eq!(line.next().unwrap(), Some(4));
    }

    #[test]
    fn poll_line_poll_increments_count() {
        let mut line = MockLine::new(noop_local_waker());
        let mut waker = noop_waker();

        assert_eq!(line.poll_count, 0);
        assert!(matches!(
            crate::Poll::poll(&mut line, &mut waker).unwrap(),
            core::task::Poll::Pending
        ));
        assert_eq!(line.poll_count, 1);
    }
}
