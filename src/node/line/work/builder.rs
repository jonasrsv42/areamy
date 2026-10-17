//! [LineBuilder] sets a work [Line]'s options before it exists.
use crate::edge::fanout::Fanout;
use crate::edge::sync::Receiver;
use crate::node::line::work::node::{Line, Types};
use core::marker::PhantomData;
use core::num::NonZeroUsize;

/// Options for a work [Line], from [Line::builder]. They are set before the line exists because
/// its input mints the senders every producer holds.
pub struct LineBuilder<'params, T: Types> {
    bound: Option<NonZeroUsize>,
    // Carries the line's types so they are inferred from its use, as with [Line::of]. `fn() ->`
    // keeps the builder `Send + Sync` whatever the routine is.
    _line: PhantomData<fn() -> Line<'params, T>>,
}

impl<'params, T: Types> LineBuilder<'params, T> {
    pub(crate) fn new() -> Self {
        LineBuilder {
            bound: None,
            _line: PhantomData,
        }
    }

    /// Hold at most `bound` messages in the line's input, signals included; producers wait while
    /// it is full. `None` is unbounded, the default.
    pub fn bounded(mut self, bound: impl Into<Option<NonZeroUsize>>) -> Self {
        self.bound = bound.into();
        self
    }

    /// The line, running `worker`.
    pub fn build(self, worker: T::Routine) -> Line<'params, T> {
        let input = match self.bound {
            Some(bound) => Receiver::bounded(bound),
            None => Receiver::new(),
        };
        Line {
            worker,
            workers: Vec::new(),
            outputs: Fanout::new(),
            input,
            pending: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::{Get, TryPush};
    use crate::message::Message;
    use crate::node::line::routine::tests::MockWaitLine;
    use crate::node::line::work::node::Node;
    use crate::signal::Trackable;
    use crate::thread::DefaultThread;
    use crate::work::Line;
    use crate::work::Sink;
    use core::num::NonZeroUsize;

    type TestSignal = Trackable<&'static str>;

    #[test]
    fn bounded_line_refuses_past_its_bound() {
        let line: Line<Node<usize, usize, TestSignal, DefaultThread, MockWaitLine>> =
            Line::builder()
                .bounded(NonZeroUsize::MIN)
                .build(MockWaitLine::new(1));
        let mut input: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Get::get(&line).unwrap();
        assert_eq!(input.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(
            input.try_push(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
    }
}
