use super::{Shared, SharedState, Types};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::node::poll::Drive;
use crate::poll::Context;
use crate::poll::Pollable;

/// Work phase: polls routine async work, transitions flush/close states.
pub struct Work<T: Types> {
    pub(crate) shared: Shared<T>,
}

impl<T: Types> Work<T> {
    pub(super) fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }
}

impl<T: Types> Connection for Work<T> {}

impl<T: Types> Pollable for Work<T> {
    type ThreadId = T::ThreadId;

    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        let SharedState {
            work,
            output,
            state,
            ..
        } = &mut *s;
        Drive {
            routine: &mut work.target,
            output: &output.waker,
            state,
        }
        .poll(cx)
    }
}
