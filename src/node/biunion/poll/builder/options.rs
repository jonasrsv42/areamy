//! [BiunionBuilder] sets a poll biunion's options before it exists, from
//! `thread.builder().biunion()`.

use super::node::Node;
use super::types::{PollBiunion, Types};
use crate::node::biunion::{Inputs, Side};
use crate::poll::edge::Deferred;
use crate::poll::{Budget, Builder, Thread};
use crate::thread::ThreadId;
use core::num::NonZeroUsize;

/// A poll biunion's options, carried by its builder until its edges exist.
#[derive(Default, Clone, Copy)]
pub(crate) struct Options {
    /// Messages each input edge of a side holds; `None` is unbounded.
    pub(crate) bound: Inputs<Option<NonZeroUsize>, Option<NonZeroUsize>>,
    /// One input budget, shared by both sides.
    pub(crate) budget: Budget,
}

/// Options for a poll biunion, from `thread.builder().biunion()`; [BiunionBuilder::build] makes
/// the node. Set before the node exists: each input mints its producers' senders at once.
#[must_use = "build the biunion with `.build(factory)`"]
pub struct BiunionBuilder<'thread, 'params, ThreadIdType: ThreadId> {
    thread: &'thread mut Thread<'params, ThreadIdType>,
    options: Options,
}

impl<'thread, 'params, ThreadIdType: ThreadId> Builder<'thread, 'params, ThreadIdType> {
    pub fn biunion(self) -> BiunionBuilder<'thread, 'params, ThreadIdType> {
        BiunionBuilder {
            thread: self.thread,
            options: Options::default(),
        }
    }
}

impl<'thread, 'params, ThreadIdType: ThreadId> BiunionBuilder<'thread, 'params, ThreadIdType> {
    /// Hold at most `bound` messages per input edge of side `S`, signals included: its sync
    /// bridge, or each parent's edge. Its producers wait while it is full: until Input drains it,
    /// or, once the routine refuses, until the routine makes room. Each side is bounded
    /// independently. `None` is unbounded, the default.
    ///
    /// Close is fast: under backpressure, what is still in flight past the routine is dropped.
    /// Flush before closing to deliver everything.
    pub fn bounded<S: Side>(mut self, bound: impl Into<Option<NonZeroUsize>>) -> Self {
        let Inputs { left, right } = &mut self.options.bound;
        *S::pick(left, right) = bound.into();
        self
    }

    /// Messages the node's Inputs take and its Output delivers per poll.
    pub fn budget(mut self, budget: Budget) -> Self {
        self.options.budget = budget;
        self
    }

    /// The node, running the routine `factory` makes; wire its edges as with `thread.biunion`.
    pub fn build<Left, Right, Out, SignalType, FactoryType>(
        self,
        factory: FactoryType,
    ) -> Node<
        'params,
        Deferred,
        Deferred,
        Deferred,
        PollBiunion<Left, Right, Out, SignalType, ThreadIdType, FactoryType>,
    >
    where
        PollBiunion<Left, Right, Out, SignalType, ThreadIdType, FactoryType>:
            Types<'params, Factory = FactoryType>,
    {
        Node::with_options(factory, self.thread.waker_allocator(), self.options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::biunion::{Left, Right};
    use crate::poll::Thread;

    crate::thread_id!(TestThread);

    #[test]
    fn bounded_sets_only_its_side_and_budget_the_options() {
        let mut thread = Thread::<TestThread>::new();
        let budget = Budget::default().output(NonZeroUsize::MIN);
        let two = NonZeroUsize::MIN.saturating_add(1);
        let options = thread
            .builder()
            .biunion()
            .bounded::<Left>(NonZeroUsize::MIN)
            .bounded::<Right>(two)
            .budget(budget)
            .options;
        assert_eq!(options.bound.left, Some(NonZeroUsize::MIN));
        assert_eq!(options.bound.right, Some(two));
        assert_eq!(options.budget, budget);
    }
}
