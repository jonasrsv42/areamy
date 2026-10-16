use super::{Shared, SharedState, Types};
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::node::poll::Drive;
use crate::poll::Context;
use crate::poll::Pollable;

pub struct Work<T: Types> {
    shared: Shared<T>,
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
        let mut s = self
            .shared
            .try_borrow_mut()
            .map_err(|_| crate::fatal!("node state borrowed reentrantly"))?;
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
