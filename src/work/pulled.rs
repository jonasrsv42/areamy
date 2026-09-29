use crate::edge::fanout::Fanout;
use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::Connection;
use crate::pull::Pullable;
use crate::work::Sink;
use crate::work::Workable;
use std::any::type_name;

/// [`Pulled`] runs a [Pullable] chain as a work node: each [Workable::work] pulls one message
/// and pushes it into every connected child.
///
/// The pull → work boundary is an ordinary connection:
///
/// ```ignore
/// let frontend = root.then(Framer::new()).then(Mel::new());
/// work::Bidi::connect(work::Pulled::of(frontend), &mut model)?;
/// ```
pub struct Pulled<'params, PullableType: Pullable> {
    pullable: PullableType,
    /// A refused message waits here, and nothing new is pulled until it is delivered.
    outputs: Fanout<'params, PullableType::DataType, PullableType::SignalType>,
}

impl<'params, PullableType: Pullable> Pulled<'params, PullableType> {
    /// Wrap `pullable`; data, signal and thread types come from it.
    pub fn of(pullable: PullableType) -> Self {
        Pulled {
            pullable,
            outputs: Fanout::default(),
        }
    }
}

impl<PullableType: Pullable> Connection for Pulled<'_, PullableType> {}

impl<PullableType> Workable for Pulled<'_, PullableType>
where
    PullableType: Pullable,
    PullableType::DataType: Clone,
    PullableType::SignalType: Clone,
{
    type ThreadId = PullableType::ThreadId;

    /// One message per round: finish a suspended push, else pull one and push it. Never pulls
    /// while suspended: a pull runs the whole chain, and its result would have nowhere to go.
    ///
    /// A closed chain returns `Closed` without closing its outputs: dropping this node drops
    /// them, and a child's edge closes once every producer is gone (fan-in safe).
    fn work(&mut self) -> Result<(), Error> {
        let Some(ready) = self.outputs.ready() else {
            // Done for this round even if it completed: a pull may block on the source while
            // the owner already has this message.
            let _ = self.outputs.resume(type_name::<PullableType>())?;
            return Ok(());
        };
        // A refused message stays in the fan-out; the next round resumes it.
        let _ = ready.push(self.pullable.pull()?)?;
        Ok(())
    }
}

impl<'params, PullableType: Pullable>
    Add<
        dyn Sink<DataType = PullableType::DataType, SignalType = PullableType::SignalType>
            + Send
            + Sync
            + 'params,
    > for Pulled<'params, PullableType>
{
    fn add(
        &mut self,
        sink: Box<
            dyn Sink<DataType = PullableType::DataType, SignalType = PullableType::SignalType>
                + Send
                + Sync
                + 'params,
        >,
    ) -> Result<(), Error> {
        self.outputs.add(sink);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::edge::sync::Receiver;
    use crate::edge::sync::tests::{join_within, wait_until};
    use crate::error::Error;
    use crate::graph::Add;
    use crate::graph::Pushable;
    use crate::graph::marker::Connection;
    use crate::node::line::routine::tests::{MockLine, MockWaitLine};
    use crate::pull::WriterBuffer;
    use crate::work::Sink;
    use crate::work::{self, Line, Reader, Workable, Writer};
    use crate::{DefaultThread, Message, Pullable, Push, Trackable};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    type TestSignal = Trackable<&'static str>;

    /// Pulls 0, 1, 2, … and counts its pulls.
    struct Counter {
        next: usize,
        pulls: Arc<AtomicUsize>,
    }

    impl Connection for Counter {}

    impl Pullable for Counter {
        type ThreadId = DefaultThread;
        type DataType = usize;
        type SignalType = TestSignal;

        fn pull(&mut self) -> Result<Message<usize, TestSignal>, Error> {
            self.pulls.fetch_add(1, Ordering::Relaxed);
            self.next += 1;
            Ok(Message::Data(self.next - 1))
        }
    }

    fn counter() -> (Counter, Arc<AtomicUsize>) {
        let pulls = Arc::new(AtomicUsize::new(0));
        let counter = Counter {
            next: 0,
            pulls: pulls.clone(),
        };
        (counter, pulls)
    }

    /// Maps a counter's `n` to `2n + offset`, so two counters stay distinguishable.
    struct Tag<P>(P, usize);

    impl<P> Connection for Tag<P> {}

    impl<P: Pullable<DataType = usize>> Pullable for Tag<P> {
        type ThreadId = P::ThreadId;
        type DataType = usize;
        type SignalType = P::SignalType;

        fn pull(&mut self) -> Result<Message<usize, P::SignalType>, Error> {
            Ok(match self.0.pull()? {
                Message::Data(n) => Message::Data(2 * n + self.1),
                other => other,
            })
        }
    }

    #[test]
    fn two_pulled_parents_share_a_bounded_owner() {
        let (a, a_pulls) = counter();
        let (b, b_pulls) = counter();
        let mut line = Line::builder()
            .bounded(NonZeroUsize::MIN)
            .build(MockWaitLine::new(1));
        // `a` yields evens, `b` odds. Whoever pushes second is refused (`Full`) every round.
        work::Bidi::connect(work::Pulled::of(Tag(a, 0)), &mut line).unwrap();
        work::Bidi::connect(work::Pulled::of(Tag(b, 1)), &mut line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        // A parent blocking on the owner's input, filled by its sibling, would deadlock here.
        let reading = thread::spawn(move || {
            (0..100)
                .map(|_| reader.read().unwrap().data().unwrap())
                .collect::<Vec<usize>>()
        });
        let got = join_within(reading);

        // Both progressed, each in order, nothing lost or duplicated.
        let (evens, odds): (Vec<usize>, Vec<usize>) = got.iter().partition(|n| *n % 2 == 0);
        assert_eq!(evens, (0..evens.len()).map(|n| 2 * n).collect::<Vec<_>>());
        assert_eq!(odds, (0..odds.len()).map(|n| 2 * n + 1).collect::<Vec<_>>());
        assert!(evens.len() >= 40 && odds.len() >= 40, "{got:?}");
        // Every pull is delivered or held, at most one held per parent.
        let pulls = a_pulls.load(Ordering::Relaxed) + b_pulls.load(Ordering::Relaxed);
        assert!((100..=102).contains(&pulls), "{pulls} pulls for 100 reads");
    }

    #[test]
    fn suspended_pulled_does_not_pull() {
        let (counter, pulls) = counter();
        let mut pulled = work::Pulled::of(counter);
        let side = Receiver::<usize, TestSignal>::bounded(NonZeroUsize::MIN);
        let edge: Box<dyn Sink<DataType = usize, SignalType = TestSignal> + Send + Sync> =
            Box::new(side.sender());
        Add::add(&mut pulled, edge).unwrap();

        pulled.work().unwrap();
        // Refused with `Full`: yields, holding message 1.
        pulled.work().unwrap();
        assert_eq!(pulls.load(Ordering::Relaxed), 2);

        // `Stuck`: this round blocks until a pop, and delivers the held message without pulling.
        let working = thread::spawn(move || pulled.work().map(|()| pulled));
        wait_until(|| side.producers_waiting() == 1);
        assert_eq!(side.poll().unwrap(), Some(Message::Data(0)));
        let mut pulled = join_within(working).unwrap();
        assert_eq!(pulls.load(Ordering::Relaxed), 2);
        assert_eq!(side.poll().unwrap(), Some(Message::Data(1)));

        // Delivered: the next round pulls again.
        pulled.work().unwrap();
        assert_eq!(pulls.load(Ordering::Relaxed), 3);
        assert_eq!(side.poll().unwrap(), Some(Message::Data(2)));
    }

    #[test]
    fn pulled_chain_fans_out_to_bidi_and_push_children() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();

        let mut pulled = work::Pulled::of(buffer.then(MockLine::new()));
        let other = Line::of(MockLine::new());
        Push::connect(&mut pulled, &other).unwrap();

        let mut line = Line::of(MockLine::new());
        work::Bidi::connect(pulled, &mut line).unwrap();

        let mut reader = Reader::new(line).unwrap();
        let mut other_reader = Reader::new(other).unwrap();

        writer.push(Message::Data(1)).unwrap();

        // 1 → pull MockLine 2 → work MockLine 4, on both branches.
        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(other_reader.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn pulled_close_closes_children() {
        let buffer = WriterBuffer::new();
        let writer = Writer::new(&buffer).unwrap();

        let mut line = Line::of(MockLine::new());
        work::Bidi::connect(work::Pulled::of(buffer), &mut line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        drop(writer);

        assert!(reader.read().is_err());
    }
}
