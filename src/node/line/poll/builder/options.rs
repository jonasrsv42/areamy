//! [LineBuilder] sets a poll line's options before it exists, from `thread.builder().line()`.

use super::node::Node;
use super::types::{PollLine, Types};
use crate::poll::edge::Deferred;
use crate::poll::{Budget, Builder, Thread};
use crate::thread::ThreadId;
use std::num::NonZeroUsize;

/// A poll line's options, carried by its builder until its edges exist.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Options {
    /// Messages each input edge holds; `None` is unbounded.
    pub(crate) bound: Option<NonZeroUsize>,
    pub(crate) budget: Budget,
}

/// Options for a poll line, from `thread.builder().line()`; [LineBuilder::build] makes the node.
/// Set before the node exists: its input mints every producer's sender (`.input()` at
/// once), so a bound on the input covers them all.
#[must_use = "build the line with `.build(factory)`"]
pub struct LineBuilder<'thread, 'params, ThreadIdType: ThreadId> {
    thread: &'thread mut Thread<'params, ThreadIdType>,
    options: Options,
}

impl<'thread, 'params, ThreadIdType: ThreadId> Builder<'thread, 'params, ThreadIdType> {
    pub fn line(self) -> LineBuilder<'thread, 'params, ThreadIdType> {
        LineBuilder {
            thread: self.thread,
            options: Options::default(),
        }
    }
}

impl<'thread, 'params, ThreadIdType: ThreadId> LineBuilder<'thread, 'params, ThreadIdType> {
    /// Hold at most `bound` messages per input edge, signals included: the sync bridge from
    /// `.input()`, or each parent's edge from `p.via(..)`, each bounded on its own. Its
    /// producers wait while it is full: until Input drains it, or, once the routine refuses, until
    /// the routine makes room. `None` is unbounded, the default.
    ///
    /// Close is fast: under backpressure, what is still in flight past the routine is dropped.
    /// Flush before closing to deliver everything.
    pub fn bounded(mut self, bound: impl Into<Option<NonZeroUsize>>) -> Self {
        self.options.bound = bound.into();
        self
    }

    /// Messages the node's Input takes and its Output delivers per poll.
    pub fn budget(mut self, budget: Budget) -> Self {
        self.options.budget = budget;
        self
    }

    /// The node, running the routine `factory` makes; wire its edges as with `thread.line`.
    pub fn build<InType, OutType, SignalType, FactoryType>(
        self,
        factory: FactoryType,
    ) -> Node<
        'params,
        Deferred,
        Deferred,
        PollLine<InType, OutType, SignalType, ThreadIdType, FactoryType>,
    >
    where
        PollLine<InType, OutType, SignalType, ThreadIdType, FactoryType>:
            Types<'params, Factory = FactoryType>,
    {
        Node::with_options(factory, self.thread.waker_allocator(), self.options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poll::{BUDGET, Thread};

    crate::thread_id!(TestThread);

    #[test]
    fn options_default_to_unbounded_with_the_default_budget() {
        let mut thread = Thread::<TestThread>::new();
        let options = thread.builder().line().options;
        assert_eq!(options.bound, None);
        assert_eq!(options.budget.input.get(), BUDGET);
        assert_eq!(options.budget.output.get(), BUDGET);
    }

    #[test]
    fn bounded_and_budget_set_the_options() {
        let mut thread = Thread::<TestThread>::new();
        let budget = Budget::default().input(NonZeroUsize::MIN);
        let options = thread
            .builder()
            .line()
            .bounded(NonZeroUsize::MIN)
            .budget(budget)
            .options;
        assert_eq!(options.bound, Some(NonZeroUsize::MIN));
        assert_eq!(options.budget, budget);
    }
}
