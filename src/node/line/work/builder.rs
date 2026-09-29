//! [LineBuilder] sets a work [Line]'s options before it exists.
use crate::edge::fanout::Fanout;
use crate::edge::sync::Receiver;
use crate::node::line::routine::LineRoutine;
use crate::node::line::work::node::Line;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// Options for a work [Line], from [Line::builder]. They are set before the line exists because
/// its input mints the senders every producer holds.
pub struct LineBuilder<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    bound: Option<NonZeroUsize>,
    // Carries the line's types so they are inferred from its use, as with [Line::of]. `fn() ->`
    // keeps the builder `Send + Sync` whatever the routine is.
    _line: PhantomData<fn() -> Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>>,
}

impl<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
    LineBuilder<'params, In, Out, SignalType, ThreadIdType, LineRoutineType>
where
    In: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    LineRoutineType: LineRoutine<In, Out> + 'params,
{
    pub(crate) fn new() -> Self {
        LineBuilder {
            bound: None,
            _line: PhantomData,
        }
    }

    /// Hold at most `bound` messages in the line's input, signals included; producers wait while
    /// it is full.
    pub fn bounded(mut self, bound: NonZeroUsize) -> Self {
        self.bound = Some(bound);
        self
    }

    /// The line, running `worker`.
    pub fn build(
        self,
        worker: LineRoutineType,
    ) -> Line<'params, In, Out, SignalType, ThreadIdType, LineRoutineType> {
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
    use crate::signal::Trackable;
    use crate::thread::DefaultThread;
    use crate::work::Line;
    use crate::work::Sink;
    use std::num::NonZeroUsize;

    type TestSignal = Trackable<&'static str>;

    #[test]
    fn bounded_line_refuses_past_its_bound() {
        let line: Line<usize, usize, TestSignal, DefaultThread, MockWaitLine> = Line::builder()
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
