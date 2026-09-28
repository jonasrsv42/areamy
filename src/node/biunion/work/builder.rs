//! [BiunionBuilder] sets a work [Biunion]'s options before it exists.
use crate::edge::fanout::Fanout;
use crate::node::biunion::Side;
use crate::node::biunion::routine::BiunionRoutine;
use crate::node::biunion::work::node::{Biunion, Input, Worker};
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// Options for a work [Biunion], from [Biunion::builder]. They are set before the node exists
/// because its inputs mint the senders every producer holds.
pub struct BiunionBuilder<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    left: Option<NonZeroUsize>,
    right: Option<NonZeroUsize>,
    // Carries the node's types so they are inferred from its use, as with [Biunion::of].
    // `fn() ->` keeps the builder `Send + Sync` whatever the routine is.
    _biunion: PhantomData<
        fn() -> Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>,
    >,
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    BiunionBuilder<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
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
    pub fn build(
        self,
        routine: RoutineType,
    ) -> Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> {
        Biunion {
            routine,
            worker: Worker::default(),
            outputs: Fanout::new(),
            input: Input::new(self.left, self.right),
            pending: None,
        }
    }
}
