//! Mocks shared across the test modules.

use crate::wake::LocalWaker;
use alloc::collections::VecDeque;

/// A sync poll routine's output: each push wakes the Output phase (the poll routine contract).
pub(super) struct WakingOutput {
    out: VecDeque<usize>,
    output: LocalWaker,
}

impl WakingOutput {
    pub(super) fn new(output: LocalWaker) -> Self {
        Self {
            out: VecDeque::new(),
            output,
        }
    }

    pub(super) fn push(&mut self, item: usize) {
        self.out.push_back(item);
        self.output.wake();
    }

    pub(super) fn pop(&mut self) -> Option<usize> {
        self.out.pop_front()
    }
}
