use super::{Shared, SharedState, Types};
use crate::error::Error;
use crate::graph::marker::{Connection, Unary};
use crate::node::poll::Emit;
use crate::poll::Pollable;
use crate::poll::waker::Waker;

pub struct Output<T: Types> {
    shared: Shared<T>,
}

impl<T: Types> Output<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }
}

impl<T: Types> Connection for Output<T> {}

impl<T: Types> Pollable for Output<T> {
    type ThreadId = T::ThreadId;

    fn poll(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        let SharedState {
            input,
            work,
            output,
            state,
        } = &mut *s;
        Emit {
            routine: &mut work.target,
            output,
            inputs: &[&input.left.waker, &input.right.waker],
            state,
        }
        .poll::<Unary>(waker)
    }
}
