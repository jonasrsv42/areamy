//! [`BiunionRoutine`] for poll biunion nodes — two typed inputs, one output.
//!
//! Same contract as [line's LineRoutine](crate::poll::LineRoutine)
//! but with two [routine::TrySend] impls dispatched via [Left](biunion::Left)
//! and [Right](biunion::Right) markers.
//!
//! See [line's routine docs](crate::node::line::poll::routine) for the full
//! contract (try_send, flush, poll, output waking).
//!
//! ## Per side
//!
//! Each side is its own Input phase holding its own refused message. After
//! [routine::Accept::Full] on a side, wake that side's Input
//! ([BiunionInputs](super::factory::BiunionInputs) `left` / `right`) once it has
//! room. A routine with one budget for both sides wakes every side it refused.
//! Sides drain independently: one side holding never stops the other.

use crate::node::{biunion, routine};

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a poll biunion routine from (`{Left}`, `{Right}`) to `{Out}`",
    note = "needs the supertraits of `poll::BiunionRoutine` and an explicit `impl poll::BiunionRoutine<{Left}, {Right}, {Out}> for {Self}`"
)]
pub trait BiunionRoutine<Left, Right, Out>:
    routine::TrySend<Left, biunion::Left>
    + routine::TrySend<Right, biunion::Right>
    + routine::Next<Out>
    + routine::Flush
    + routine::Poll
{
}

#[cfg(test)]
pub mod tests {
    use crate::Accept;
    use crate::biunion;
    use crate::error::Error;
    use crate::poll::Context;
    use crate::poll::context::mock::noop_context;
    use crate::wake::LocalWaker;
    use crate::wake::mock::noop_local_waker;
    use alloc::collections::VecDeque;

    /// Mock biunion routine: left input doubled, right input tripled,
    /// both pushed to output. Tracks poll_count.
    pub struct MockBiunion {
        output: VecDeque<usize>,
        /// Output's waker, woken per output.
        waker: LocalWaker,
        pub poll_count: usize,
        flushed: bool,
    }

    impl MockBiunion {
        pub fn new(wakers: crate::poll::BiunionWakers) -> Self {
            MockBiunion {
                output: VecDeque::new(),
                waker: wakers.output,
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
            work: noop_context(),
            output: noop_local_waker(),
        }
    }

    impl crate::TrySend<usize, biunion::Left> for MockBiunion {
        fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
            self.output.push_back(message * 2);
            self.waker.wake();
            Ok(Accept::Accepted)
        }
    }

    impl crate::TrySend<usize, biunion::Right> for MockBiunion {
        fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
            self.output.push_back(message * 3);
            self.waker.wake();
            Ok(Accept::Accepted)
        }
    }

    impl crate::Next<usize> for MockBiunion {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.output.pop_front())
        }
    }

    impl crate::Flush for MockBiunion {
        fn flush(&mut self) -> Result<(), Error> {
            self.flushed = true;
            Ok(())
        }
    }

    impl crate::Poll for MockBiunion {
        fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
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
