//! [`BiunionRoutine`] for poll biunion nodes — two typed inputs, one output.
//!
//! Same contract as [line's LineRoutine](crate::node::line::poll::routine::LineRoutine)
//! but with two [routine::Send] impls dispatched via [Left](biunion::Left)
//! and [Right](biunion::Right) markers.
//!
//! See [line's routine docs](crate::node::line::poll::routine) for the full
//! contract (flush, poll, output waking).

use crate::node::{biunion, routine};

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a poll biunion routine from (`{Left}`, `{Right}`) to `{Out}`",
    note = "needs the supertraits of `poll::BiunionRoutine` and an explicit `impl poll::BiunionRoutine<{Left}, {Right}, {Out}> for {Self}`"
)]
pub trait BiunionRoutine<Left, Right, Out>:
    routine::Send<Left, biunion::Left>
    + routine::Send<Right, biunion::Right>
    + routine::Next<Out>
    + routine::Flush
    + routine::Poll
{
}

#[cfg(test)]
pub mod tests {
    use crate::biunion;
    use crate::error::Error;
    use crate::poll::future::queue::OutputQueue;
    use crate::poll::waker::Waker;
    use crate::poll::waker::mock::noop_local_waker;

    /// Mock biunion routine: left input doubled, right input tripled,
    /// both pushed to output. Tracks poll_count.
    pub struct MockBiunion {
        output: OutputQueue<usize>,
        pub poll_count: usize,
        flushed: bool,
    }

    impl MockBiunion {
        pub fn new(wakers: crate::poll::BiunionWakers) -> Self {
            MockBiunion {
                output: OutputQueue::new(wakers.output),
                poll_count: 0,
                flushed: false,
            }
        }
    }

    pub fn noop_biunion_wakers() -> crate::poll::BiunionWakers {
        crate::poll::BiunionWakers {
            input: crate::poll::BiunionInputs {
                left: noop_local_waker(),
                right: noop_local_waker(),
            },
            work: noop_local_waker(),
            output: noop_local_waker(),
        }
    }

    impl crate::Send<usize, biunion::Left> for MockBiunion {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.output.producer.push(message * 2);
            Ok(())
        }
    }

    impl crate::Send<usize, biunion::Right> for MockBiunion {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.output.producer.push(message * 3);
            Ok(())
        }
    }

    impl crate::Next<usize> for MockBiunion {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.output.consumer.pop())
        }
    }

    impl crate::Flush for MockBiunion {
        fn flush(&mut self) -> Result<(), Error> {
            self.flushed = true;
            Ok(())
        }
    }

    impl crate::Poll for MockBiunion {
        fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
            self.poll_count += 1;
            if self.flushed {
                self.flushed = false;
                return Ok(core::task::Poll::Ready(()));
            }
            Ok(core::task::Poll::Pending)
        }
    }

    impl super::BiunionRoutine<usize, usize, usize> for MockBiunion {}
}
