//! [BifurcationBuilder] sets a work [Bifurcation]'s options before it exists.
use crate::edge::sync::Receiver;
use crate::node::bifurcation::Sides;
use crate::node::bifurcation::work::node::{Bifurcation, Types};
use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// Options for a work [Bifurcation], from [Bifurcation::builder]. They are set before the node
/// exists because its input mints the senders every producer holds.
pub struct BifurcationBuilder<'params, T: Types> {
    bound: Option<NonZeroUsize>,
    // Carries the node's types so they are inferred from its use, as with [Bifurcation::of].
    // `fn() ->` keeps the builder `Send + Sync` whatever the routine is.
    _bifurcation: PhantomData<fn() -> Bifurcation<'params, T>>,
}

impl<'params, T: Types> BifurcationBuilder<'params, T> {
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
    pub fn build(self, routine: T::Routine) -> Bifurcation<'params, T> {
        let input = match self.bound {
            Some(bound) => Receiver::bounded(bound),
            None => Receiver::new(),
        };
        Bifurcation {
            routine,
            workers: Vec::new(),
            outputs: Sides::default(),
            input,
            pending: Sides::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::{Get, TryPush};
    use crate::message::Message;
    use crate::node::bifurcation::routine::tests::HoldBifurcation;
    use crate::node::bifurcation::work::node::Node;
    use crate::signal::Trackable;
    use crate::thread::DefaultThread;
    use crate::work::Bifurcation;
    use crate::work::Sink;
    use std::num::NonZeroUsize;

    type TestSignal = Trackable<&'static str>;
    type Hold =
        Bifurcation<'static, Node<usize, usize, usize, TestSignal, DefaultThread, HoldBifurcation>>;

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
