use core::num::NonZeroUsize;

/// Messages one poll may take from an edge before it yields, re-waking itself so nothing is
/// stranded.
///
/// The answer to life, the universe and fair scheduling: enough to amortise a poll's fixed
/// cost, few enough that no node hogs the thread. Don't panic.
pub const BUDGET: usize = 42;

/// How many messages a poll node's Input takes, and its Output delivers, per poll before
/// yielding to the rest of the thread. Both default to [BUDGET]; never zero, which would spin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub(crate) input: NonZeroUsize,
    pub(crate) output: NonZeroUsize,
}

impl Budget {
    /// [BUDGET] for both.
    pub const DEFAULT: Self = {
        let budget = match NonZeroUsize::new(BUDGET) {
            Some(budget) => budget,
            None => NonZeroUsize::MIN,
        };
        Budget {
            input: budget,
            output: budget,
        }
    };

    /// Messages the Input takes from its edge per poll.
    pub fn input(mut self, budget: NonZeroUsize) -> Self {
        self.input = budget;
        self
    }

    /// Messages the Output delivers per poll.
    pub fn output(mut self, budget: NonZeroUsize) -> Self {
        self.output = budget;
        self
    }
}

impl Default for Budget {
    fn default() -> Self {
        Self::DEFAULT
    }
}
