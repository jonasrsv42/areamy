//! [LineRoutine] is the work horse of all Line nodes. It is a frankenstein [std::ops::Coroutine].

/// [`LineRoutine`] is a flushable subset of [std::ops::Coroutine] accepting a stream of `In` types through
/// [LineRoutine::send] and produce a stream of output with [LineRoutine::next].
///
/// [std::ops::Coroutine] was not stable at time of development.

/// [LineRoutine] implements a [crate::Send] for a single input and a [crate::Next] for a single
/// output.
///
/// [crate::Send] contract:
///
/// The routine does not have to produce any output on [crate::Send] and is expected to accumulate
/// state until it can yield on [crate::Next].
///
/// After [crate::Send] is invoked [crate::Next] will be invoked
/// until it yields [Option::None]. Then [crate::Send] will be invoked
/// again and so it may repeat.
///
/// The Routine will loop like that, potentially forever.
///
/// [crate::Flush] contract:
///
/// [crate::Flush] signals to the routine that it should output any state it can into
/// subsequent [crate::Next] and then reset all of its internal state for future
/// [crate::Send] invocations.
///
/// <div class="warning"> The routine should never reset its internal output buffer </div>
///
/// It should only reset all other state associated with processing. In other words:
/// a [crate::Flush] call should only ever create, potentially premature, additional output. A
/// [crate::Flush] call should not remove any output.
///
/// [crate::Next] contract
///
/// [crate::Next] yields the next output available from the [LineRoutine]. If no
/// more output can be yielded without additional [crate::Send] it should yield
/// [Option::None].
///
/// [crate::Send] for a [LineRoutine] cannot be invoked again without [crate::Next]
/// having yielded [Option::None].
///
/// [crate::Next] must only yield [Option::None] if it requires additional
/// [crate::Send] to produce more output. The function should be blocking.
///
pub trait LineRoutine<In, Out>:
    Send + crate::Send<In> + crate::Next<Out> + crate::Flush + crate::node::Name
{
}

/// [`AsyncLineRoutine`] is the async counterpart of [LineRoutine].
///
/// Prefer [FutureRoutine](crate::poll::FutureRoutine) over raw impls —
/// it enforces all contracts below by construction.
///
/// Unlike [LineRoutine], does NOT require [std::marker::Send]. Async
/// routines are created on the async thread via
/// [PollLineRoutineFactory](crate::node::line::poll::factory::PollLineRoutineFactory)
/// and never cross threads. This allows routines to hold non-Send types
/// like `Rc<RefCell<_>>` for zero-cost shared state with their futures.
///
/// The factory receives a [ThreadLocalWaker](crate::connect::waker::ThreadLocalWaker)
/// for the Output phase. Routines MUST use this to wake Output when they
/// produce data (e.g. via [OutputProducer](crate::poll::future::queue::OutputProducer)).
///
/// ## [crate::Send] contract
///
/// If the routine needs async processing to handle data received
/// via [crate::Send], it MUST arrange for [crate::Poll] to be woken
/// (e.g. via a waker stored during a previous [crate::Poll] call).
/// Failing to do so will deadlock — [crate::Poll] is not automatically
/// invoked after [crate::Send].
///
/// Routines that produce output synchronously in [crate::Send]
/// do not need to wake [crate::Poll].
///
/// [FutureRoutine](crate::poll::FutureRoutine) handles this via its
/// waker-aware [InputQueue](crate::poll::future::queue::InputQueue) —
/// push wakes Poll automatically.
///
/// ## [crate::Next] contract
///
/// The node's Output phase calls [crate::Next] to drain output. Output
/// is NOT polled automatically — the routine MUST wake the Output phase
/// when it produces data. Use [OutputProducer::push](crate::poll::future::queue::OutputProducer::push)
/// which wakes Output via [ThreadLocalWaker](crate::connect::waker::ThreadLocalWaker).
///
/// ## [crate::Poll] contract
///
/// [crate::Poll::poll] receives a [Waker](crate::connect::waker::Waker)
/// carrying both a sync waker (for I/O / standard futures) and a
/// thread-local waker (for cheap same-thread wake).
///
/// - [core::task::Poll::Pending] — async work in progress.
/// - [core::task::Poll::Ready] — routine finished processing after
///   [crate::Flush].
///
/// After [crate::Flush], [crate::Poll] is invoked once. The routine
/// MUST either return Ready immediately, or return Pending and arrange
/// to be woken and return Ready on a subsequent invocation. Failing
/// to eventually return Ready will deadlock.
///
/// Returning Ready outside of a flush/close is a fatal error.
///
/// Returning [crate::error::ErrorKind::Closed] is a fatal error.
///
/// ## [crate::Flush] contract
///
/// [crate::Flush] signals the routine to finalize its current segment.
/// The routine should output any remaining data via [crate::Next] and
/// reset its state for subsequent [crate::Send] invocations.
///
/// After [crate::Flush], [crate::Poll] is invoked once. If the routine
/// returns Ready, the flush is complete. If it returns Pending, the
/// runtime will NOT wake the routine again — the routine must arrange
/// for itself to be woken (e.g. by handing its waker to an I/O source,
/// timer, or interrupt that will fire). The node will be waiting for
/// a Ready signal to eventually arrive. Failure to wake itself leads
/// to deadlock.
///
/// ## TL;DR
///
/// - Output data? Wake Output via [OutputProducer](crate::poll::future::queue::OutputProducer) or the factory's [ThreadLocalWaker](crate::connect::waker::ThreadLocalWaker). Output is NOT polled automatically.
/// - Need async work after [crate::Send]? Wake Work (e.g. via [InputQueue](crate::poll::future::queue::InputQueue) push).
/// - After [crate::Flush], return [core::task::Poll::Ready] from [crate::Poll] (immediately or eventually). Deadlock otherwise.
/// - Never return [core::task::Poll::Ready] outside flush. Fatal error.
/// - Never return [crate::error::ErrorKind::Closed] from [crate::Poll]. Fatal error — the routine does not manage its own lifecycle.
///
/// Think this is too many rules? Just use a premade wrapper like [FutureRoutine](crate::poll::FutureRoutine) it will handle all of it for you.
pub trait AsyncLineRoutine<In, Out>:
    crate::Send<In> + crate::Next<Out> + crate::Flush + crate::Poll + crate::node::Name
{
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::connect::waker::{ThreadLocalWake, ThreadLocalWaker, Waker};
    use crate::error::Error;
    use crate::poll::future::queue::OutputQueue;
    use crate::{Next, Send};
    use std::collections::VecDeque;

    pub struct MockLine {
        state: usize,
        out: VecDeque<usize>,
    }

    impl MockLine {
        pub fn new() -> Self {
            MockLine {
                state: 0,
                out: VecDeque::new(),
            }
        }
    }

    impl crate::Send<usize> for MockLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.state += message;
            self.out.push_back(self.state * 2);

            Ok(())
        }
    }

    impl crate::Next<usize> for MockLine {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.out.pop_front())
        }
    }

    impl crate::Flush for MockLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.state = 0;
            Ok(())
        }
    }

    impl crate::node::Name for MockLine {}

    impl LineRoutine<usize, usize> for MockLine {}

    pub struct AccMockLine {
        num: Vec<usize>,
        out: VecDeque<Vec<usize>>,
    }

    impl crate::node::Name for AccMockLine {}

    impl AccMockLine {
        pub fn new() -> Self {
            AccMockLine {
                num: Vec::new(),
                out: VecDeque::new(),
            }
        }
    }

    impl crate::Send<usize> for AccMockLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.num.push(message);

            if self.num.len() == 2 {
                self.out.push_back(self.num.clone());
                self.num.clear();
            }

            Ok(())
        }
    }

    impl crate::Next<Vec<usize>> for AccMockLine {
        fn next(&mut self) -> Result<Option<Vec<usize>>, Error> {
            Ok(self.out.pop_front())
        }
    }

    impl crate::Flush for AccMockLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.num.clear();
            Ok(())
        }
    }

    impl LineRoutine<usize, Vec<usize>> for AccMockLine {}

    pub struct MockWaitLine {
        out: VecDeque<usize>,

        /// [MockWaitLine::release] indicats that we can release all output.
        release: bool,

        /// Wait for [MockWaitLine::wait] before release is true.
        wait: usize,
    }

    impl MockWaitLine {
        pub fn new(wait: usize) -> Self {
            MockWaitLine {
                out: VecDeque::new(),
                release: false,
                wait,
            }
        }
    }

    impl crate::Send<usize> for MockWaitLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.out.push_back(message);
            self.release = self.out.len() >= self.wait;

            Ok(())
        }
    }

    impl crate::Next<usize> for MockWaitLine {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            if self.release {
                Ok(self.out.pop_front())
            } else {
                Ok(None)
            }
        }
    }

    impl crate::Flush for MockWaitLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.release = true;
            Ok(())
        }
    }

    impl crate::node::Name for MockWaitLine {}
    impl LineRoutine<usize, usize> for MockWaitLine {}

    #[test]
    fn line_basic_work() {
        let mut line = MockLine::new();
        line.send(2).unwrap();

        assert_eq!(line.next().unwrap(), Some(4));
    }
    #[test]
    fn line_basic_acc_work() {
        let mut line = AccMockLine::new();
        line.send(2).unwrap();
        assert_eq!(line.next().unwrap(), None);
        line.send(3).unwrap();

        assert_eq!(line.next().unwrap(), Some(vec![2, 3]));
    }

    #[test]
    fn line_basic_wait_work() {
        let mut line = MockWaitLine::new(4);
        line.send(2).unwrap();
        line.send(3).unwrap();
        line.send(4).unwrap();

        assert_eq!(line.next().unwrap(), None);
        line.send(5).unwrap();
        assert_eq!(line.next().unwrap(), Some(2));
        assert_eq!(line.next().unwrap(), Some(3));
        assert_eq!(line.next().unwrap(), Some(4));
        assert_eq!(line.next().unwrap(), Some(5));
    }

    /// Mock async line routine that processes input like MockLine
    /// and tracks poll_count to verify waking behavior.
    pub struct AsyncMockLine {
        state: usize,
        output: OutputQueue<usize>,
        pub poll_count: usize,
        flushed: bool,
    }

    impl AsyncMockLine {
        pub fn new(output_waker: ThreadLocalWaker) -> Self {
            AsyncMockLine {
                state: 0,
                output: OutputQueue::new(output_waker),
                poll_count: 0,
                flushed: false,
            }
        }
    }

    impl crate::Send<usize> for AsyncMockLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.state += message;
            self.output.producer.push(self.state * 2);
            Ok(())
        }
    }

    impl crate::Next<usize> for AsyncMockLine {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.output.consumer.pop())
        }
    }

    impl crate::Flush for AsyncMockLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.state = 0;
            self.flushed = true;
            Ok(())
        }
    }

    impl crate::Poll for AsyncMockLine {
        fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
            self.poll_count += 1;
            if self.flushed {
                self.flushed = false;
                return Ok(core::task::Poll::Ready(()));
            }
            Ok(core::task::Poll::Pending)
        }
    }

    impl crate::node::Name for AsyncMockLine {}
    impl AsyncLineRoutine<usize, usize> for AsyncMockLine {}

    #[test]
    fn async_line_send_next_works() {
        let mut line = AsyncMockLine::new(noop_local_waker());
        line.send(2).unwrap();
        assert_eq!(line.next().unwrap(), Some(4));
    }

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
    fn async_line_poll_increments_count() {
        let mut line = AsyncMockLine::new(noop_local_waker());
        let mut waker = noop_waker();

        assert_eq!(line.poll_count, 0);
        assert!(matches!(
            crate::Poll::poll(&mut line, &mut waker).unwrap(),
            core::task::Poll::Pending
        ));
        assert_eq!(line.poll_count, 1);
    }
}
