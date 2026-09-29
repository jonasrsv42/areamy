//! [BifurcationBuilder] sets a work [Bifurcation]'s options before it exists.
use crate::edge::fanout::Fanout;
use crate::edge::sync::Receiver;
use crate::node::bifurcation::routine::BifurcationRoutine;
use crate::node::bifurcation::work::node::{Bifurcation, Sides};
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// Options for a work [Bifurcation], from [Bifurcation::builder]. They are set before the node
/// exists because its input mints the senders every producer holds.
pub struct BifurcationBuilder<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    bound: Option<NonZeroUsize>,
    // Carries the node's types so they are inferred from its use, as with [Bifurcation::of].
    // `fn() ->` keeps the builder `Send + Sync` whatever the routine is.
    _bifurcation: PhantomData<
        fn() -> Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>,
    >,
}

impl<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
    BifurcationBuilder<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right> + 'params,
{
    pub(crate) fn new() -> Self {
        BifurcationBuilder {
            bound: None,
            _bifurcation: PhantomData,
        }
    }

    /// Hold at most `bound` messages in the input, signals included; producers wait while it
    /// is full.
    pub fn bounded(mut self, bound: NonZeroUsize) -> Self {
        self.bound = Some(bound);
        self
    }

    /// The node, running `routine`.
    pub fn build(
        self,
        routine: RoutineType,
    ) -> Bifurcation<'params, In, Left, Right, SignalType, ThreadIdType, RoutineType> {
        let input = match self.bound {
            Some(bound) => Receiver::bounded(bound),
            None => Receiver::new(),
        };
        Bifurcation {
            routine,
            workers: Vec::new(),
            outputs: Sides {
                left: Fanout::new(),
                right: Fanout::new(),
            },
            input,
            pending: Sides {
                left: None,
                right: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::{Get, TryPush};
    use crate::message::Message;
    use crate::node::bifurcation::routine::tests::HoldBifurcation;
    use crate::signal::Trackable;
    use crate::thread::DefaultThread;
    use crate::work::Bifurcation;
    use crate::work::Sink;
    use std::num::NonZeroUsize;

    type TestSignal = Trackable<&'static str>;
    type Hold =
        Bifurcation<'static, usize, usize, usize, TestSignal, DefaultThread, HoldBifurcation>;

    #[test]
    fn bounded_bifurcation_refuses_past_its_bound() {
        let bifurcation: Hold = Bifurcation::builder()
            .bounded(NonZeroUsize::MIN)
            .build(HoldBifurcation::new(1));
        let mut input: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Get::get(&bifurcation).unwrap();
        assert_eq!(input.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(
            input.try_push(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
    }
}
