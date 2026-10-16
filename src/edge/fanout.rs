//! [Fanout] pushes each message into N edges and holds the one an edge refused.
use crate::edge::deadlock::deadlock;
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Outlet, Pushable, Schedule, TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Origin;
use crate::wake;

/// Outcome of pushing one message through a [Fanout].
#[must_use = "a suspended fan-out must be resumed before anything new is pushed"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rotation {
    /// Every edge took the message.
    Complete,
    /// This edge refused; the fan-out holds the message until [Fanout::resume] or
    /// [Fanout::retry] delivers it.
    Suspended(usize),
}

impl Rotation {
    pub fn is_suspended(self) -> bool {
        matches!(self, Rotation::Suspended(_))
    }
}

/// Edge `index` refused `refused`; `rest` is the source for the edges after it, `None` when
/// `index` is the last edge.
struct Suspended<DataType, SignalType: Origin> {
    index: usize,
    refused: Message<DataType, SignalType>,
    rest: Option<Message<DataType, SignalType>>,
}

/// The message type an edge carries.
type MessageOf<EdgeType> =
    Message<<EdgeType as Outlet>::DataType, <EdgeType as Outlet>::SignalType>;

/// Pushes each message into every edge: clones for all but the last, which takes the move, so
/// a single edge never clones. A refusal stops the rotation at that edge; the fan-out keeps the
/// message, and nothing new can be pushed until the rotation completes.
///
/// What it can do follows its edges: rotation and [Fanout::retry] need [TryPushable],
/// [Fanout::resume] (which may block) also [Pushable], [Fanout::close] [Closeable]. Edges that
/// can't block can't be resumed:
///
/// ```compile_fail,E0599
/// use areamy::edge::fanout::Fanout;
/// type Signal = areamy::Trackable<&'static str>;
/// type Edge = Box<dyn areamy::poll::Sink<DataType = usize, SignalType = Signal>>;
/// fn resume(fanout: &mut Fanout<Edge>) {
///     let _ = fanout.resume("node");
/// }
/// ```
pub struct Fanout<EdgeType: Outlet> {
    edges: Vec<EdgeType>,
    suspended: Option<Suspended<EdgeType::DataType, EdgeType::SignalType>>,
}

/// A [Fanout] with no suspended rotation, from [Fanout::ready].
pub struct Ready<'fanout, EdgeType: Outlet> {
    fanout: &'fanout mut Fanout<EdgeType>,
}

impl<EdgeType> Ready<'_, EdgeType>
where
    EdgeType: TryPushable,
    EdgeType::DataType: Clone,
    EdgeType::SignalType: Clone,
{
    /// Start a rotation. Consumes the handle: after a [Rotation::Suspended], the next
    /// [Fanout::ready] is `None`.
    pub fn push(self, message: MessageOf<EdgeType>) -> Result<Rotation, Error> {
        self.fanout.rotate(0, message)
    }
}

impl<EdgeType: Outlet> Default for Fanout<EdgeType> {
    fn default() -> Self {
        Fanout {
            edges: Vec::new(),
            suspended: None,
        }
    }
}

/// A fan-out of one: the edge always takes the move.
impl<EdgeType: Outlet> From<EdgeType> for Fanout<EdgeType> {
    fn from(edge: EdgeType) -> Self {
        Fanout {
            edges: vec![edge],
            suspended: None,
        }
    }
}

impl<EdgeType: Outlet> Fanout<EdgeType> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, edge: EdgeType) {
        self.edges.push(edge);
    }

    /// The edge holding up a suspended rotation.
    pub fn suspended(&self) -> Option<usize> {
        self.suspended.as_ref().map(|suspended| suspended.index)
    }

    /// A handle to push a new message; `None` while suspended, so the held message can't be
    /// overtaken.
    pub fn ready(&mut self) -> Option<Ready<'_, EdgeType>> {
        match self.suspended {
            Some(_) => None,
            None => Some(Ready { fanout: self }),
        }
    }

    /// A suspended index always names an edge: edges are only ever added.
    fn edge(&mut self, index: usize) -> Result<&mut EdgeType, Error> {
        self.edges
            .get_mut(index)
            .ok_or_else(|| fatal!("fan-out suspended on a missing edge {}", index))
    }

    fn suspend(
        &mut self,
        index: usize,
        refused: MessageOf<EdgeType>,
        rest: Option<MessageOf<EdgeType>>,
    ) -> Rotation {
        self.suspended = Some(Suspended {
            index,
            refused,
            rest,
        });
        Rotation::Suspended(index)
    }
}

impl<EdgeType: Outlet + Closeable> Fanout<EdgeType> {
    /// Close every edge, even if one fails; returns the first error. A held message is dropped.
    pub fn close(&mut self) -> Result<(), Error> {
        self.suspended = None;
        let mut result = Ok(());
        for edge in &mut self.edges {
            if let Err(error) = edge.close()
                && result.is_ok()
            {
                result = Err(error);
            }
        }
        result
    }
}

impl<EdgeType: Outlet> Connection for Fanout<EdgeType> {}

/// Binds every edge to `waker`, one clone each: after a [Rotation::Suspended], it is woken
/// once the edge holding the rotation takes more or closes; then [Fanout::retry].
impl<EdgeType: Outlet + Schedule> Schedule for Fanout<EdgeType> {
    fn schedule(&mut self, waker: wake::Waker) {
        if let Some((last, rest)) = self.edges.split_last_mut() {
            for edge in rest {
                edge.schedule(waker.clone());
            }
            last.schedule(waker);
        }
    }
}

/// Delivering a message clones it for every edge but the last.
impl<EdgeType> Fanout<EdgeType>
where
    EdgeType: TryPushable,
    EdgeType::DataType: Clone,
    EdgeType::SignalType: Clone,
{
    /// Continue a suspended rotation without ever blocking: `Full` and `Stuck` both suspend
    /// again. For callers that must not block.
    pub fn retry(&mut self) -> Result<Rotation, Error> {
        let Some(suspended) = self.suspended.take() else {
            return Ok(Rotation::Complete);
        };
        let edge = self.edge(suspended.index)?;
        match edge.try_push(suspended.refused)?.refused() {
            None => self.rotate_rest(suspended.index, suspended.rest),
            Some(refused) => Ok(self.suspend(suspended.index, refused, suspended.rest)),
        }
    }

    fn rotate_rest(
        &mut self,
        index: usize,
        rest: Option<MessageOf<EdgeType>>,
    ) -> Result<Rotation, Error> {
        match rest {
            Some(message) => self.rotate(index + 1, message),
            None => Ok(Rotation::Complete),
        }
    }

    /// Deliver `message` to the edges from `from` on.
    fn rotate(&mut self, from: usize, message: MessageOf<EdgeType>) -> Result<Rotation, Error> {
        let Some((last, rest)) = self
            .edges
            .get_mut(from..)
            .and_then(|edges| edges.split_last_mut())
        else {
            return Ok(Rotation::Complete);
        };
        let last_index = from + rest.len();
        for (offset, edge) in rest.iter_mut().enumerate() {
            // The refused clone comes back; the original stays the source for the rest.
            if let Some(refused) = edge.try_push(message.clone())?.refused() {
                return Ok(self.suspend(from + offset, refused, Some(message)));
            }
        }
        match last.try_push(message)?.refused() {
            None => Ok(Rotation::Complete),
            Some(refused) => Ok(self.suspend(last_index, refused, None)),
        }
    }
}

/// Edges that can block.
impl<EdgeType> Fanout<EdgeType>
where
    EdgeType: TryPushable + Pushable,
    EdgeType::DataType: Clone,
    EdgeType::SignalType: Clone,
{
    /// Continue a suspended rotation. `Full` suspends again: the consumer drained since the
    /// last refusal, so yielding lets it drain more. `Stuck` blocks, logging "blocked" /
    /// "unblocked" under `node`: nothing drained, so yielding again would spin.
    pub fn resume(&mut self, node: &str) -> Result<Rotation, Error> {
        let Some(suspended) = self.suspended.take() else {
            return Ok(Rotation::Complete);
        };
        let edge = self.edge(suspended.index)?;
        match edge.try_push(suspended.refused)? {
            TryPush::Pushed => {}
            TryPush::Full(refused) => {
                return Ok(self.suspend(suspended.index, refused, suspended.rest));
            }
            TryPush::Stuck(refused) => {
                deadlock(node, suspended.index, || edge.push(refused))?;
            }
        }
        // The later edges only get a try: one may suspend the rotation again.
        self.rotate_rest(suspended.index, suspended.rest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Trackable;
    use crate::edge::sync::Receiver;
    use crate::edge::sync::tests::{join_within, wait_until};
    use crate::error::ErrorKind;
    use crate::graph::Receivable;
    use crate::graph::tests::{Counted, FailingClose};
    use crate::poll;
    use crate::poll::sink::mock::ScheduleOnly;
    use crate::wake::mock::{noop_local_waker, noop_waker};
    use crate::work::DynSink;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    type Signal = Trackable<&'static str>;

    const NODE: &str = "test";

    /// One receiver per bound (`None` is unbounded) and a fan-out over their senders.
    fn fan<D: Clone + Send + Sync + 'static>(
        bounds: &[Option<usize>],
    ) -> (
        Fanout<DynSink<'static, D, Signal>>,
        Vec<Receiver<D, Signal>>,
    ) {
        let receivers: Vec<Receiver<D, Signal>> = bounds
            .iter()
            .map(|bound| match bound.and_then(NonZeroUsize::new) {
                Some(bound) => Receiver::bounded(bound),
                None => Receiver::new(),
            })
            .collect();
        let mut fanout: Fanout<DynSink<'static, D, Signal>> = Fanout::new();
        for receiver in &receivers {
            fanout.add(Box::new(receiver.sender()));
        }
        (fanout, receivers)
    }

    fn clones_for(outputs: usize) -> usize {
        let clones = Arc::new(AtomicUsize::new(0));
        let (mut fanout, receivers) = fan::<Counted>(&vec![None; outputs]);
        let rotation = fanout
            .ready()
            .unwrap()
            .push(Message::Data(Counted(clones.clone())))
            .unwrap();
        assert_eq!(rotation, Rotation::Complete);
        for receiver in &receivers {
            assert!(matches!(receiver.poll().unwrap(), Some(Message::Data(_))));
        }
        clones.load(Ordering::Relaxed)
    }

    #[test]
    fn clones_all_but_last() {
        assert_eq!(clones_for(0), 0);
        assert_eq!(clones_for(1), 0);
        assert_eq!(clones_for(3), 2);
    }

    #[test]
    fn suspension_adds_no_clones() {
        let clones = Arc::new(AtomicUsize::new(0));
        let (mut fanout, receivers) = fan::<Counted>(&[None, Some(1), None]);
        // Fill the middle edge with an uncounted message.
        let filler = Counted(Arc::new(AtomicUsize::new(0)));
        receivers[1]
            .sender()
            .push_back(Message::Data(filler))
            .unwrap();

        let rotation = fanout
            .ready()
            .unwrap()
            .push(Message::Data(Counted(clones.clone())))
            .unwrap();
        assert_eq!(rotation, Rotation::Suspended(1));
        receivers[1].poll().unwrap();
        assert_eq!(fanout.retry().unwrap(), Rotation::Complete);

        // Same as an unsuspended rotation over three edges.
        assert_eq!(clones.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn refusal_suspends_at_the_refusing_edge() {
        let (mut fanout, receivers) = fan::<usize>(&[None, Some(1), None]);
        receivers[1].sender().push_back(Message::Data(0)).unwrap();

        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(7)).unwrap(),
            Rotation::Suspended(1)
        );
        assert_eq!(fanout.suspended(), Some(1));
        assert_eq!(receivers[0].poll().unwrap(), Some(Message::Data(7)));
        // Nothing past the refusal yet.
        assert_eq!(receivers[2].poll().unwrap(), None);
    }

    #[test]
    fn not_ready_while_suspended() {
        let (mut fanout, receivers) = fan::<usize>(&[Some(1)]);
        receivers[0].sender().push_back(Message::Data(0)).unwrap();
        assert!(fanout.ready().is_some());
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(1)).unwrap(),
            Rotation::Suspended(0)
        );
        assert!(fanout.ready().is_none());

        // The held message goes out, then the fan-out is ready again.
        receivers[0].poll().unwrap();
        assert_eq!(fanout.retry().unwrap(), Rotation::Complete);
        assert_eq!(receivers[0].poll().unwrap(), Some(Message::Data(1)));
        assert!(fanout.ready().is_some());
    }

    #[test]
    fn retry_never_blocks_on_stuck() {
        let (mut fanout, receivers) = fan::<usize>(&[Some(1)]);
        receivers[0].sender().push_back(Message::Data(0)).unwrap();
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(1)).unwrap(),
            Rotation::Suspended(0)
        );

        // No progress since the refusal: the edge answers `Stuck`, and retry still returns.
        let retrier = thread::spawn(move || fanout.retry().map(|rotation| (fanout, rotation)));
        let (fanout, rotation) = join_within(retrier).unwrap();
        assert_eq!(rotation, Rotation::Suspended(0));
        assert_eq!(fanout.suspended(), Some(0));
    }

    #[test]
    fn resume_yields_on_full_after_progress() {
        let (mut fanout, receivers) = fan::<usize>(&[Some(1)]);
        let other = receivers[0].sender();
        other.push_back(Message::Data(0)).unwrap();
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(1)).unwrap(),
            Rotation::Suspended(0)
        );

        // The consumer drained, but another producer refilled: `Full`, so yield, don't block.
        receivers[0].poll().unwrap();
        other.push_back(Message::Data(2)).unwrap();
        let resumer = thread::spawn(move || fanout.resume(NODE).map(|rotation| (fanout, rotation)));
        let (_fanout, rotation) = join_within(resumer).unwrap();
        assert_eq!(rotation, Rotation::Suspended(0));
    }

    #[test]
    fn resume_blocks_on_stuck_until_a_pop_then_finishes() {
        let (mut fanout, receivers) = fan::<usize>(&[Some(1), None]);
        // Kept alive: dropping the last sender would close the edge.
        let filler = receivers[0].sender();
        filler.push_back(Message::Data(0)).unwrap();
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(7)).unwrap(),
            Rotation::Suspended(0)
        );

        let resumer = thread::spawn(move || fanout.resume(NODE).map(|rotation| (fanout, rotation)));
        wait_until(|| receivers[0].producers_waiting() == 1);
        assert_eq!(receivers[0].poll().unwrap(), Some(Message::Data(0)));

        let (_fanout, rotation) = join_within(resumer).unwrap();
        assert_eq!(rotation, Rotation::Complete);
        assert_eq!(receivers[0].poll().unwrap(), Some(Message::Data(7)));
        // The rotation carried on past the blocked edge.
        assert_eq!(receivers[1].poll().unwrap(), Some(Message::Data(7)));
    }

    #[test]
    fn closed_edge_errors_without_suspending() {
        let (mut fanout, mut receivers) = fan::<usize>(&[None, None, None]);
        drop(receivers.remove(1));

        let error = fanout.ready().unwrap().push(Message::Data(7)).unwrap_err();
        assert!(matches!(error.kind, ErrorKind::Closed));
        assert_eq!(fanout.suspended(), None);
        assert_eq!(receivers[0].poll().unwrap(), Some(Message::Data(7)));
        // The edge after the closed one never got it.
        assert_eq!(receivers[1].poll().unwrap(), None);
    }

    /// Suspend at edge 1, drain it, and retry: the rotation must suspend again at `expected`,
    /// a later edge, and after draining that one deliver exactly one copy everywhere.
    fn suspends_again_after_retry(bounds: &[Option<usize>], expected: usize) {
        let (mut fanout, receivers) = fan::<usize>(bounds);
        // Kept alive: dropping the last sender would close the edge.
        let fillers: Vec<_> = receivers.iter().map(|receiver| receiver.sender()).collect();
        fillers[1].push_back(Message::Data(0)).unwrap();
        fillers[expected].push_back(Message::Data(0)).unwrap();

        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(7)).unwrap(),
            Rotation::Suspended(1)
        );
        receivers[1].poll().unwrap();
        assert_eq!(fanout.retry().unwrap(), Rotation::Suspended(expected));
        receivers[expected].poll().unwrap();
        assert_eq!(fanout.retry().unwrap(), Rotation::Complete);

        for receiver in &receivers {
            assert_eq!(receiver.poll().unwrap(), Some(Message::Data(7)));
            assert_eq!(receiver.poll().unwrap(), None);
        }
    }

    #[test]
    fn middle_edge_suspends_after_retry() {
        suspends_again_after_retry(&[None, Some(1), Some(1), None], 2);
    }

    #[test]
    fn last_edge_suspends_after_retry() {
        suspends_again_after_retry(&[None, Some(1), Some(1)], 2);
    }

    #[test]
    fn resume_on_full_keeps_the_later_edges() {
        let (mut fanout, receivers) = fan::<usize>(&[None, Some(1), None]);
        let other = receivers[1].sender();
        other.push_back(Message::Data(0)).unwrap();
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(7)).unwrap(),
            Rotation::Suspended(1)
        );

        // Progress plus a refill: `Full`, so resume suspends again, still owing edge 2.
        receivers[1].poll().unwrap();
        other.push_back(Message::Data(0)).unwrap();
        assert_eq!(fanout.resume(NODE).unwrap(), Rotation::Suspended(1));
        receivers[1].poll().unwrap();
        assert_eq!(fanout.retry().unwrap(), Rotation::Complete);
        assert_eq!(receivers[2].poll().unwrap(), Some(Message::Data(7)));
    }

    #[test]
    fn close_while_blocked_errors_without_suspending() {
        let (mut fanout, mut receivers) = fan::<usize>(&[Some(1), None]);
        let filler = receivers[0].sender();
        filler.push_back(Message::Data(0)).unwrap();
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(7)).unwrap(),
            Rotation::Suspended(0)
        );

        // Hands the fan-out back even on error: dropping it would close the later edge.
        let resumer = thread::spawn(move || {
            let result = fanout.resume(NODE);
            (fanout, result)
        });
        wait_until(|| receivers[0].producers_waiting() == 1);
        drop(receivers.remove(0));

        let (fanout, result) = join_within(resumer);
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
        assert_eq!(fanout.suspended(), None);
        assert_eq!(receivers[0].poll().unwrap(), None);
    }

    #[test]
    fn close_closes_every_edge_and_returns_the_first_error() {
        let receiver = Receiver::<usize, Signal>::new();
        let mut fanout: Fanout<DynSink<'static, usize, Signal>> = Fanout::new();
        fanout.add(Box::new(FailingClose("first")));
        fanout.add(Box::new(FailingClose("second")));
        fanout.add(Box::new(receiver.sender()));

        let error = fanout.close().unwrap_err();
        assert!(matches!(error.kind, ErrorKind::Fatal(message) if message == "first"));
        // The edge after both failures was still closed.
        assert!(matches!(
            receiver.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    /// A concrete, same-thread edge: no boxing, no `Pushable`.
    #[test]
    fn retry_on_a_concrete_poll_edge() {
        let (sender, mut receiver) =
            poll::edge::channel::<usize, Signal>(noop_local_waker(), NonZeroUsize::new(1));
        let mut fanout = Fanout::new();
        fanout.add(sender);
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(1)).unwrap(),
            Rotation::Complete
        );
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(2)).unwrap(),
            Rotation::Suspended(0)
        );
        assert_eq!(fanout.retry().unwrap(), Rotation::Suspended(0));
        assert_eq!(receiver.try_recv().unwrap(), Some(Message::Data(1)));
        assert_eq!(fanout.retry().unwrap(), Rotation::Complete);
        assert_eq!(receiver.try_recv().unwrap(), Some(Message::Data(2)));
    }

    /// Boxed poll sinks rotate and retry like work sinks.
    #[test]
    fn retry_on_boxed_poll_sinks() {
        type PollSink = Box<dyn poll::Sink<DataType = usize, SignalType = Signal> + Send + Sync>;
        let receivers = [
            Receiver::<usize, Signal>::new(),
            Receiver::bounded(NonZeroUsize::MIN),
        ];
        // Kept alive: dropping the last sender would close the edge.
        let filler = receivers[1].sender();
        filler.push_back(Message::Data(0)).unwrap();
        let mut fanout: Fanout<PollSink> = Fanout::new();
        for receiver in &receivers {
            fanout.add(Box::new(receiver.sender()));
        }
        assert_eq!(
            fanout.ready().unwrap().push(Message::Data(7)).unwrap(),
            Rotation::Suspended(1)
        );
        receivers[1].poll().unwrap();
        assert_eq!(fanout.retry().unwrap(), Rotation::Complete);
        for receiver in &receivers {
            assert_eq!(receiver.poll().unwrap(), Some(Message::Data(7)));
        }
    }

    #[test]
    fn schedule_binds_every_edge_once() {
        let edges = [
            ScheduleOnly::default(),
            ScheduleOnly::default(),
            ScheduleOnly::default(),
        ];
        let mut fanout = Fanout::new();
        for edge in &edges {
            fanout.add(edge.clone());
        }
        Schedule::schedule(&mut fanout, noop_waker());
        for edge in &edges {
            assert_eq!(edge.schedules.load(Ordering::SeqCst), 1);
        }
    }

    fn _assert_send_sync() {
        fn require_send_sync<T: Send + Sync>() {}
        require_send_sync::<Fanout<DynSink<'static, usize, Signal>>>();
        require_send_sync::<Ready<'static, DynSink<'static, usize, Signal>>>();
    }
}
