//! [BiunionBuilder] sets a work [Biunion]'s options before it exists.
use crate::edge::fanout::Fanout;
use crate::node::biunion::Inputs;
use crate::node::biunion::Side;
use crate::node::biunion::work::node::{Biunion, Input, Types};
use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// Options for a work [Biunion], from [Biunion::builder]. They are set before the node exists
/// because its inputs mint the senders every producer holds.
pub struct BiunionBuilder<'params, T: Types> {
    left: Option<NonZeroUsize>,
    right: Option<NonZeroUsize>,
    // Carries the node's types so they are inferred from its use, as with [Biunion::of].
    // `fn() ->` keeps the builder `Send + Sync` whatever the routine is.
    _biunion: PhantomData<fn() -> Biunion<'params, T>>,
}

impl<'params, T: Types> BiunionBuilder<'params, T> {
    pub(crate) fn new() -> Self {
        BiunionBuilder {
            left: None,
            right: None,
            _biunion: PhantomData,
        }
    }

    /// Hold at most `bound` messages in side `S`'s input, signals included; its producers wait
    /// while it is full. Each side is bounded independently.
    pub fn bounded<S: Side>(mut self, bound: NonZeroUsize) -> Self {
        *S::pick(&mut self.left, &mut self.right) = Some(bound);
        self
    }

    /// The node, running `routine`.
    pub fn build(self, routine: T::Routine) -> Biunion<'params, T> {
        Biunion {
            routine,
            workers: Inputs::default(),
            outputs: Fanout::new(),
            input: Input::new(self.left, self.right),
            pending: None,
        }
    }
}
