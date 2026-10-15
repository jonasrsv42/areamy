//! [`LineRoutine`] is the poll/async counterpart of
//! [LineRoutine](crate::node::line::routine::LineRoutine) (sync).
//!
//! Prefer [FutureRoutine](crate::node::line::poll::future::FutureRoutine) over raw impls —
//! it enforces all contracts below by construction.
//!
//! Unlike the sync [LineRoutine](crate::node::line::routine::LineRoutine), does NOT require
//! [std::marker::Send]. Async routines are created on the async thread via
//! [LineRoutineFactory](super::factory::LineRoutineFactory) and never cross
//! threads. This allows routines to hold non-Send types like `Rc<RefCell<_>>`
//! for zero-cost shared state with their futures.
//!
//! The factory receives a [LocalWaker](crate::wake::LocalWaker)
//! for the Output phase. Routines MUST use this to wake Output when they
//! produce data (e.g. via [OutputProducer](crate::poll::future::queue::OutputProducer)).
//!
//! ## [routine::TrySend] contract
//!
//! [routine::TrySend::try_send] never blocks. A full routine returns
//! [routine::Accept::Full] with the message and MUST wake the Input phase
//! ([LineWakers::input](super::factory::LineWakers)) once room frees (e.g. in
//! [routine::Next] or [routine::Poll]). Wake once per refusal, after room frees;
//! waking while still full just burns a poll. The node retries the held message
//! before taking anything else. A routine that never refuses never wakes Input.
//!
//! Refuse with nothing accepted only if something other than input will free
//! room, otherwise the wake never comes. [routine::Flush] cannot refuse: it may
//! arrive while full and must be absorbed, overshooting the bound if need be.
//!
//! If the routine needs async processing to handle data received
//! via [routine::TrySend], it MUST arrange for [routine::Poll] to be woken
//! (e.g. via a waker stored during a previous [routine::Poll] call).
//! Failing to do so will deadlock — [routine::Poll] is not automatically
//! invoked after [routine::TrySend].
//!
//! Routines that produce output synchronously in [routine::TrySend]
//! do not need to wake [routine::Poll].
//!
//! [FutureRoutine](crate::node::line::poll::future::FutureRoutine) handles this via its
//! waker-aware [InputQueue](crate::poll::future::queue::InputQueue) —
//! push wakes Poll automatically.
//!
//! ## [routine::Next] contract
//!
//! The node's Output phase calls [routine::Next] to drain output. Output
//! is NOT polled automatically — the routine MUST wake the Output phase
//! when it produces data. Use [OutputProducer::push](crate::poll::future::queue::OutputProducer::push)
//! which wakes Output via [LocalWaker](crate::wake::LocalWaker).
//!
//! ## [routine::Poll] contract
//!
//! [routine::Poll::poll] receives the node's [Context](crate::poll::Context):
//! its wakers and its timer.
//!
//! - [core::task::Poll::Pending] — async work in progress.
//! - [core::task::Poll::Ready] — routine finished processing after
//!   [routine::Flush].
//!
//! After [routine::Flush], [routine::Poll] is invoked once. If the routine
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
//! - Output data? Wake Output via [OutputProducer](crate::poll::future::queue::OutputProducer) or the factory's [LocalWaker](crate::wake::LocalWaker). Output is NOT polled automatically.
//! - Full? Return [routine::Accept::Full] and wake Input once there is room.
//! - Need async work after [routine::TrySend]? Wake Work (e.g. via [InputQueue](crate::poll::future::queue::InputQueue) push).
//! - After [routine::Flush], return [core::task::Poll::Ready] from [routine::Poll] (immediately or eventually). Deadlock otherwise.
//! - Never return [core::task::Poll::Ready] outside flush. Fatal error.
//! - Never return [crate::error::ErrorKind::Closed] from [routine::Poll]. Fatal error — the routine does not manage its own lifecycle.
//!
//! Think this is too many rules? Just use [FutureRoutine](crate::node::line::poll::future::FutureRoutine) — it handles all of it for you.

use crate::node::routine;

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a poll line routine from `{In}` to `{Out}`",
    note = "needs the supertraits of `poll::LineRoutine` and an explicit `impl poll::LineRoutine<{In}, {Out}> for {Self}`"
)]
pub trait LineRoutine<In, Out>:
    routine::TrySend<In> + routine::Next<Out> + routine::Flush + routine::Poll
{
}

#[cfg(test)]
pub mod tests {
    use super::LineRoutine;
    use crate::error::Error;
    use crate::poll::Context;
    use crate::poll::context::mock::noop_context;
    use crate::poll::future::queue::OutputQueue;
    use crate::poll::timer::mock::noop_timer;
    use crate::wake::mock::noop_local_waker;
    use crate::{Accept, Next, TrySend};

    pub struct MockLine {
        state: usize,
        output: OutputQueue<usize>,
        pub poll_count: usize,
        flushed: bool,
    }

    impl MockLine {
        pub fn new(wakers: crate::poll::LineWakers) -> Self {
            MockLine {
                state: 0,
                output: OutputQueue::new(wakers.output),
                poll_count: 0,
                flushed: false,
            }
        }
    }

    impl crate::TrySend<usize> for MockLine {
        fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
            self.state += message;
            self.output.producer.push(self.state * 2);
            Ok(Accept::Accepted)
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
        fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
            self.poll_count += 1;
            if self.flushed {
                self.flushed = false;
                return Ok(core::task::Poll::Ready(()));
            }
            Ok(core::task::Poll::Pending)
        }
    }

    impl LineRoutine<usize, usize> for MockLine {}

    pub fn noop_line_wakers() -> crate::poll::LineWakers {
        crate::poll::LineWakers {
            input: noop_local_waker(),
            work: noop_local_waker(),
            output: noop_local_waker(),
            timer: noop_timer(),
        }
    }

    #[test]
    fn poll_line_send_next_works() {
        let mut line = MockLine::new(noop_line_wakers());
        assert_eq!(line.try_send(2).unwrap(), Accept::Accepted);
        assert_eq!(line.next().unwrap(), Some(4));
    }

    #[test]
    fn poll_line_poll_increments_count() {
        let mut line = MockLine::new(noop_line_wakers());
        let mut cx = noop_context();

        assert_eq!(line.poll_count, 0);
        assert!(matches!(
            crate::Poll::poll(&mut line, &mut cx).unwrap(),
            core::task::Poll::Pending
        ));
        assert_eq!(line.poll_count, 1);
    }
}
