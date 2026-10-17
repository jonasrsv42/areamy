use super::{Input, Output, Work};
use crate::Accept;
use crate::DefaultThread;
use crate::Trackable;
use crate::edge;
use crate::edge::fanout::Fanout;
use crate::error::{Error, ErrorKind};
use crate::message::Message;
use crate::node::line::poll::node;
use crate::node::line::poll::routine::LineRoutine;
use crate::node::line::poll::routine::tests::{MockLine, noop_line_wakers};
use crate::poll::Context;
use crate::poll::context::mock::noop_context;
use crate::poll::input;
use crate::poll::{BUDGET, Budget, Phase, Pollable};
use crate::wake::mock::{
    counting_local_waker, counting_waker, noop_local_waker, tracking_local_waker,
};
use crate::wake::{LocalWaker, Waker};
use alloc::collections::VecDeque;
use alloc::rc::Rc;
use alloc::sync::Arc;
use core::cell::Cell;
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task;

type Signal = Trackable<&'static str>;
type Inlet = input::sync::Receiver<usize, Signal>;
type Outlet = edge::sync::Sender<usize, Signal>;
type Node<R> = node::Node<usize, usize, Signal, DefaultThread, R, Inlet, Fanout<Outlet>>;

/// A fan-out over one edge, so these tests run the `Fanout` output; the biunion's run `Single`.
fn fanout(edge: Outlet) -> Fanout<Outlet> {
    let mut fanout = Fanout::new();
    fanout.add(edge);
    fanout
}

/// A line node with the edges into and out of it.
struct Line<R: LineRoutine<usize, usize>> {
    input: Input<Node<R>>,
    work: Work<Node<R>>,
    output: Output<Node<R>>,
    upstream: input::sync::Sender<usize, Signal>,
    downstream: edge::sync::Receiver<usize, Signal>,
    /// Whether Output's waker fired.
    output_woken: Rc<Cell<bool>>,
    /// Whether Work's waker fired.
    work_woken: Rc<Cell<bool>>,
}

impl<R: LineRoutine<usize, usize>> Line<R> {
    fn new(routine: R) -> Self {
        Self::with_input_waker(routine, noop_local_waker())
    }

    fn with_input_waker(routine: R, input_waker: LocalWaker) -> Self {
        let edge = input::sync::Receiver::new(core::task::Waker::noop().clone(), None);
        let upstream = edge.sender();
        let downstream = edge::sync::Receiver::new();
        let (output_waker, output_woken) = tracking_local_waker();
        let (work_waker, work_woken) = tracking_local_waker();
        let (input, work, output) = node::Node::phases(
            Phase {
                target: edge,
                waker: input_waker,
            },
            Phase {
                target: routine,
                waker: work_waker,
            },
            fanout(downstream.sender()),
            Waker {
                sync: task::Waker::noop().clone(),
                local: output_waker,
            },
            Budget::default(),
        );
        Self {
            input,
            work,
            output,
            upstream,
            downstream,
            output_woken,
            work_woken,
        }
    }

    /// Everything that reached downstream so far.
    fn received(&self) -> Vec<Message<usize, Signal>> {
        let mut received = Vec::new();
        loop {
            match self.downstream.poll() {
                Ok(Some(message)) => received.push(message),
                Ok(None) => return received,
                Err(e) if e.is_closed() => return received,
                Err(e) => panic!("downstream failed: {e:?}"),
            }
        }
    }

    /// Poll Input, Work and Output once each; what reached downstream.
    fn cycle(&mut self) -> Vec<Message<usize, Signal>> {
        let mut cx = noop_context();
        let _ = self.input.poll(&mut cx).unwrap();
        let _ = self.work.poll(&mut cx).unwrap();
        let _ = self.output.poll(&mut cx).unwrap();
        self.received()
    }
}

/// Holds at most `room` messages, refusing past that; wakes Input once [Next] frees room after a
/// refusal, as the [TrySend](crate::TrySend) contract requires.
struct Bounded {
    queue: VecDeque<usize>,
    room: usize,
    refused: bool,
    input: LocalWaker,
    flushed: bool,
    /// `try_send` and `poll` calls, readable after the routine moves into a node.
    sends: Rc<Cell<usize>>,
    polls: Rc<Cell<usize>>,
}

impl Bounded {
    fn new(room: usize, input: LocalWaker) -> Self {
        Self {
            queue: VecDeque::new(),
            room,
            refused: false,
            input,
            flushed: false,
            sends: Rc::default(),
            polls: Rc::default(),
        }
    }
}

impl crate::TrySend<usize> for Bounded {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
        self.sends.set(self.sends.get() + 1);
        if self.queue.len() >= self.room {
            self.refused = true;
            return Ok(Accept::Full(message));
        }
        self.queue.push_back(message);
        Ok(Accept::Accepted)
    }
}

impl crate::Next<usize> for Bounded {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        let message = self.queue.pop_front();
        if message.is_some() && core::mem::take(&mut self.refused) {
            self.input.wake();
        }
        Ok(message)
    }
}

impl crate::Flush for Bounded {
    fn flush(&mut self) -> Result<(), Error> {
        self.flushed = true;
        Ok(())
    }
}

impl crate::Poll for Bounded {
    fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        self.polls.set(self.polls.get() + 1);
        Ok(match core::mem::take(&mut self.flushed) {
            true => core::task::Poll::Ready(()),
            false => core::task::Poll::Pending,
        })
    }
}

impl LineRoutine<usize, usize> for Bounded {}

/// A line over [Bounded] with `room`, and the flag its Input waker sets.
fn bounded(room: usize) -> (Line<Bounded>, Rc<Cell<bool>>) {
    let (input_waker, input_woken) = tracking_local_waker();
    let line = Line::with_input_waker(Bounded::new(room, input_waker.clone()), input_waker);
    (line, input_woken)
}

fn mock() -> MockLine {
    MockLine::new(noop_line_wakers())
}

#[test]
fn poll_processes_input_and_produces_output() {
    let mut line = Line::new(mock());
    let mut cx = noop_context();

    line.upstream.push_back(Message::Data(2)).unwrap();

    // Input drains edge → TrySend to routine
    assert!(line.input.poll(&mut cx).unwrap().is_pending());
    // Work polls routine (sync mock — no-op here)
    assert!(line.work.poll(&mut cx).unwrap().is_pending());
    // Output drains Next → pushes downstream
    assert!(line.output.poll(&mut cx).unwrap().is_pending());

    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(4)));
}

/// A [MockLine] node (running sum, doubled) draining into `outputs`. Each phase's wakes are
/// counted; Output's through its sync half, which the edges hold.
struct Parked<R: LineRoutine<usize, usize> = MockLine> {
    input: Input<Node<R>>,
    work: Work<Node<R>>,
    output: Output<Node<R>>,
    upstream: input::sync::Sender<usize, Signal>,
    input_wakes: Rc<Cell<usize>>,
    work_wakes: Rc<Cell<usize>>,
    output_wakes: Arc<AtomicUsize>,
    /// Output waking itself, through the local half.
    output_self_wakes: Rc<Cell<usize>>,
}

impl Parked {
    fn draining_into(outputs: Fanout<Outlet>) -> Self {
        Parked::with(|_| mock(), outputs)
    }
}

impl<R: LineRoutine<usize, usize>> Parked<R> {
    /// The routine is built from the node's Input waker.
    fn with(routine: impl FnOnce(LocalWaker) -> R, outputs: Fanout<Outlet>) -> Self {
        Self::budgeted(routine, outputs, Budget::default())
    }

    fn budgeted(
        routine: impl FnOnce(LocalWaker) -> R,
        outputs: Fanout<Outlet>,
        budget: Budget,
    ) -> Self {
        let edge = input::sync::Receiver::new(task::Waker::noop().clone(), None);
        let upstream = edge.sender();
        let (input_waker, input_wakes) = counting_local_waker();
        let (work_waker, work_wakes) = counting_local_waker();
        let (mut output_wake, output_wakes) = counting_waker();
        let (output_local, output_self_wakes) = counting_local_waker();
        output_wake.local = output_local;
        let routine = routine(input_waker.clone());
        let (input, work, output) = node::Node::phases(
            Phase {
                target: edge,
                waker: input_waker,
            },
            Phase {
                target: routine,
                waker: work_waker,
            },
            outputs,
            output_wake,
            budget,
        );
        Self {
            input,
            work,
            output,
            upstream,
            input_wakes,
            work_wakes,
            output_wakes,
            output_self_wakes,
        }
    }

    /// Feed `messages` and let Input and Work take them.
    fn feed(&mut self, messages: impl IntoIterator<Item = Message<usize, Signal>>) {
        for message in messages {
            self.upstream.push_back(message).unwrap();
        }
        let mut cx = noop_context();
        let _ = self.input.poll(&mut cx).unwrap();
        let _ = self.work.poll(&mut cx).unwrap();
    }

    fn emit(&mut self) {
        assert!(self.output.poll(&mut noop_context()).unwrap().is_pending());
    }

    fn output_wakes(&self) -> usize {
        self.output_wakes.load(Ordering::SeqCst)
    }
}

/// One downstream edge holding at most one message, and a node draining into it.
fn parked() -> (Parked, edge::sync::Receiver<usize, Signal>) {
    let downstream = edge::sync::Receiver::bounded(NonZeroUsize::MIN);
    (
        Parked::draining_into(fanout(downstream.sender())),
        downstream,
    )
}

/// A node with `count` outputs ready for an unbounded downstream, and maybe a signal behind them.
fn ready_to_emit(
    count: usize,
    signal: Option<Message<usize, Signal>>,
) -> (Parked, edge::sync::Receiver<usize, Signal>) {
    let downstream = edge::sync::Receiver::new();
    let mut node = Parked::draining_into(fanout(downstream.sender()));
    for value in 0..count {
        node.upstream.push_back(Message::Data(value)).unwrap();
    }
    if let Some(signal) = signal {
        node.upstream.push_back(signal).unwrap();
    }
    // Input takes at most BUDGET per poll; enough polls to take everything.
    for _ in 0..=count / BUDGET {
        node.feed([]);
    }
    (node, downstream)
}

/// Output delivers at most BUDGET per poll and wakes itself for the rest, so a routine with a
/// lot of output can't hold the poll thread.
#[test]
fn output_emits_at_most_budget_then_wakes_itself() {
    let (mut node, downstream) = ready_to_emit(BUDGET + 3, None);
    node.emit();
    assert_eq!(downstream.len().unwrap(), BUDGET);
    assert_eq!(node.output_self_wakes.get(), 1);
    node.emit();
    assert_eq!(downstream.len().unwrap(), BUDGET + 3);
    assert_eq!(node.output_self_wakes.get(), 1);
}

/// A signal behind more than BUDGET of data still goes out last.
#[test]
fn a_signal_behind_more_than_budget_lands_last() {
    let (mut node, downstream) = ready_to_emit(BUDGET + 3, Some(Message::Flush("f".into())));
    node.emit();
    assert_eq!(downstream.len().unwrap(), BUDGET);
    node.emit();
    let received = (0..=BUDGET + 3).map(|_| downstream.poll().unwrap());
    assert_eq!(received.last(), Some(Some(Message::Flush("f".into()))));
    assert_eq!(downstream.poll().unwrap(), None);
}

/// A node's own budget caps each phase: Input takes 2 per poll, Output delivers 5.
#[test]
fn a_node_budget_caps_input_and_output_separately() {
    let downstream = edge::sync::Receiver::new();
    let budget = Budget::default()
        .input(NonZeroUsize::new(2).unwrap())
        .output(NonZeroUsize::new(5).unwrap());
    let mut node = Parked::budgeted(|_| mock(), fanout(downstream.sender()), budget);
    for value in 0..10 {
        node.upstream.push_back(Message::Data(value)).unwrap();
    }
    let mut cx = noop_context();
    let _ = node.input.poll(&mut cx).unwrap();
    node.emit();
    assert_eq!(downstream.len().unwrap(), 2);

    for _ in 0..3 {
        let _ = node.input.poll(&mut cx).unwrap();
    }
    node.emit();
    assert_eq!(downstream.len().unwrap(), 7);
}

/// Close still delivers the routine's buffered output, BUDGET per poll, before closing.
#[test]
fn close_emits_buffered_output_across_polls() {
    let (mut node, downstream) = ready_to_emit(BUDGET + 3, None);
    node.upstream.close().unwrap();
    assert!(node.input.poll(&mut noop_context()).is_err());
    node.emit();
    assert!(node.output.poll(&mut noop_context()).is_err());
    assert_eq!(downstream.len().unwrap(), BUDGET + 3);
}

#[test]
fn output_fans_out_to_every_edge() {
    let first = edge::sync::Receiver::new();
    let second = edge::sync::Receiver::new();
    let mut outputs = Fanout::new();
    outputs.add(first.sender());
    outputs.add(second.sender());
    let mut node = Parked::draining_into(outputs);
    node.feed([Message::Data(1), Message::Data(2)]);
    node.emit();
    for receiver in [&first, &second] {
        assert_eq!(receiver.poll().unwrap(), Some(Message::Data(2)));
        assert_eq!(receiver.poll().unwrap(), Some(Message::Data(6)));
    }
}

/// A refusal parks Output instead of blocking; the pop wakes it, and the held message goes out
/// first.
#[test]
fn output_parks_on_a_full_edge_and_resumes_on_the_pop() {
    let (mut node, downstream) = parked();
    node.feed([Message::Data(1), Message::Data(2)]);
    node.emit();
    assert_eq!(node.output_wakes(), 0);

    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(2)));
    assert_eq!(node.output_wakes(), 1);
    node.emit();
    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(6)));
}

/// Over two edges, the second refusing: Output parks there with the first already served. The
/// second's pop wakes it, the held copy goes only to the second, and the next message parks
/// there again.
#[test]
fn output_parks_on_a_later_edge_and_resumes_there() {
    let first = edge::sync::Receiver::new();
    let second = edge::sync::Receiver::bounded(NonZeroUsize::MIN);
    let mut outputs = fanout(first.sender());
    outputs.add(second.sender());
    let mut node = Parked::draining_into(outputs);
    let drained = |edge: &edge::sync::Receiver<usize, Signal>| {
        core::iter::from_fn(|| edge.poll().unwrap()).collect::<Vec<_>>()
    };

    // 2 goes to both; 6 goes to the first and the second refuses it.
    node.feed([Message::Data(1), Message::Data(2)]);
    node.emit();
    // Parked: 12 waits in the routine, and a spurious wake delivers nothing.
    node.feed([Message::Data(3)]);
    node.emit();
    assert_eq!(drained(&first), vec![Message::Data(2), Message::Data(6)]);
    assert_eq!(node.output_wakes(), 0);

    assert_eq!(second.poll().unwrap(), Some(Message::Data(2)));
    assert_eq!(node.output_wakes(), 1);
    // 6 goes to the second only; 12 goes to the first and the second refuses it again.
    node.emit();
    assert_eq!(drained(&first), vec![Message::Data(12)]);
    assert_eq!(second.poll().unwrap(), Some(Message::Data(6)));
    node.emit();
    assert_eq!(drained(&second), vec![Message::Data(12)]);
    assert_eq!(drained(&first), vec![]);
}

#[test]
fn a_refused_flush_waits_then_resumes_the_node() {
    let (mut node, downstream) = parked();
    node.feed([
        Message::Data(1),
        Message::Flush("f".into()),
        Message::Data(5),
    ]);
    // Work turned the Flush ready; Output delivers 2 and the Flush is refused.
    node.emit();
    let (input_wakes, work_wakes) = (node.input_wakes.get(), node.work_wakes.get());

    // Forwarding: a spurious wake delivers nothing, and Input reads nothing past the Flush.
    node.emit();
    let _ = node.input.poll(&mut noop_context()).unwrap();
    let _ = node.input.poll(&mut noop_context()).unwrap();
    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(2)));
    assert_eq!(
        (node.input_wakes.get(), node.work_wakes.get()),
        (input_wakes, work_wakes)
    );

    node.emit();
    assert_eq!(downstream.poll().unwrap(), Some(Message::Flush("f".into())));
    assert_eq!(node.input_wakes.get(), input_wakes + 1);
    assert_eq!(node.work_wakes.get(), work_wakes + 1);
    // Running again: the data behind the Flush flows (the flush reset the sum).
    node.feed([]);
    node.emit();
    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(10)));
}

/// Same for a refused Marker: Forwarding until the pop, delivered once, then the node resumes.
#[test]
fn a_refused_marker_waits_then_resumes_the_node() {
    let (mut node, downstream) = parked();
    node.feed([
        Message::Data(1),
        Message::Marker("m".into()),
        Message::Data(5),
    ]);
    // Work gave the routine its poll; Output delivers 2 and the Marker is refused.
    node.emit();
    let (input_wakes, work_wakes) = (node.input_wakes.get(), node.work_wakes.get());

    // Forwarding: a spurious wake delivers nothing, and Input reads nothing past the Marker.
    node.emit();
    let _ = node.input.poll(&mut noop_context()).unwrap();
    let _ = node.input.poll(&mut noop_context()).unwrap();
    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(2)));
    assert_eq!(
        (node.input_wakes.get(), node.work_wakes.get()),
        (input_wakes, work_wakes)
    );

    node.emit();
    assert_eq!(
        downstream.poll().unwrap(),
        Some(Message::Marker("m".into()))
    );
    assert_eq!(downstream.poll().unwrap(), None);
    assert_eq!(node.input_wakes.get(), input_wakes + 1);
    assert_eq!(node.work_wakes.get(), work_wakes + 1);
    // Running again: the data behind the Marker flows (a Marker keeps the sum).
    node.feed([]);
    node.emit();
    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(12)));
}

/// Close is a fast shutdown: what an edge still refuses is dropped, and the edge closes.
#[test]
fn close_while_parked_drops_the_held_message_and_closes() {
    let (mut node, downstream) = parked();
    node.feed([Message::Data(1), Message::Data(2)]);
    node.emit();
    drop(node.upstream);
    let _ = node.input.poll(&mut noop_context());
    assert!(node.output.poll(&mut noop_context()).is_err());
    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(2)));
    assert!(downstream.poll().is_err());
}

/// The consumer closing while Output is parked wakes it; the retry finds the edge closed and
/// closes the node.
#[test]
fn a_consumer_closing_on_a_parked_output_closes_the_node() {
    let (mut node, downstream) = parked();
    node.feed([Message::Data(1), Message::Data(2)]);
    node.emit();
    drop(downstream);
    assert_eq!(node.output_wakes(), 1);
    assert!(node.output.poll(&mut noop_context()).is_err());
    assert!(node.input.poll(&mut noop_context()).is_err());
}

#[test]
fn poll_returns_closed_on_closed_input() {
    let mut line = Line::new(mock());
    let mut cx = noop_context();

    line.upstream.close().unwrap();

    // Fast close: Input detects closed → CloseReady directly,
    // skipping the Closing/poll-routine cycle. Work is irrelevant.
    let result = line.input.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert!(line.output_woken.get(), "Input must wake Output on close");

    // Output drains buffered output (none here) and closes downstream.
    let result = line.output.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
}

#[test]
fn poll_calls_routine_poll() {
    let mut line = Line::new(mock());
    let mut cx = noop_context();

    let _ = line.work.poll(&mut cx).unwrap();
    assert_eq!(line.work.shared.borrow().work.target.poll_count, 1);

    let _ = line.work.poll(&mut cx).unwrap();
    assert_eq!(line.work.shared.borrow().work.target.poll_count, 2);
}

struct ClosedErrorRoutine;

impl crate::TrySend<usize> for ClosedErrorRoutine {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        Err(crate::closed!())
    }
}

impl crate::Next<usize> for ClosedErrorRoutine {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl crate::Flush for ClosedErrorRoutine {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::Poll for ClosedErrorRoutine {
    fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        Err(crate::closed!())
    }
}

impl LineRoutine<usize, usize> for ClosedErrorRoutine {}

#[test]
fn routine_returning_closed_becomes_fatal() {
    let mut line = Line::new(ClosedErrorRoutine);
    let mut cx = noop_context();

    let err = line.work.poll(&mut cx).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn routine_returning_closed_from_try_send_becomes_fatal() {
    let mut line = Line::new(ClosedErrorRoutine);
    let mut cx = noop_context();

    line.upstream.push_back(Message::Data(1)).unwrap();

    let err = line.input.poll(&mut cx).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn routine_returning_closed_during_flush_becomes_fatal() {
    let mut line = Line::new(ClosedErrorRoutine);
    let mut cx = noop_context();

    line.upstream
        .push_back(Message::Flush("s1".into()))
        .unwrap();

    let _ = line.input.poll(&mut cx);

    let err = line.work.poll(&mut cx).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

/// Done with its async work on every poll, flush or not.
struct ReadyRoutine;

impl crate::TrySend<usize> for ReadyRoutine {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        Ok(Accept::Accepted)
    }
}

impl crate::Next<usize> for ReadyRoutine {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl crate::Flush for ReadyRoutine {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::Poll for ReadyRoutine {
    fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        Ok(core::task::Poll::Ready(()))
    }
}

impl LineRoutine<usize, usize> for ReadyRoutine {}

#[test]
fn routine_ready_while_running_is_fatal() {
    let mut line = Line::new(ReadyRoutine);
    let mut cx = noop_context();

    let err = line.work.poll(&mut cx).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn routine_ready_on_a_marker_is_fatal() {
    let mut line = Line::new(ReadyRoutine);
    let mut cx = noop_context();

    line.upstream
        .push_back(Message::Marker("m".into()))
        .unwrap();
    let _ = line.input.poll(&mut cx);

    let err = line.work.poll(&mut cx).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn poll_drains_multiple_inputs() {
    let mut line = Line::new(mock());
    let mut cx = noop_context();

    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.work.poll(&mut cx).unwrap();
    let _ = line.output.poll(&mut cx).unwrap();

    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(2)));
    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(6)));
}

#[test]
fn marker_preserves_ordering_with_data() {
    let mut line = Line::new(mock());
    let mut cx = noop_context();

    // Data then Marker then more Data
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    // First Input poll: drains Data(1), hits Marker → MarkerReady, stops
    let _ = line.input.poll(&mut cx).unwrap();

    // Poll Input again — should be blocked in MarkerReady, Data(2) stays in edge
    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.input.poll(&mut cx).unwrap();

    let _ = line.work.poll(&mut cx).unwrap();

    // Output drains data before marker, then forwards marker → Running
    let _ = line.output.poll(&mut cx).unwrap();

    // Data(1) output arrives before Marker — ordering preserved
    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(2))); // 1*2=2
    assert_eq!(
        line.downstream.poll().unwrap(),
        Some(Message::Marker("m1".into()))
    );
    // Data(2) must NOT have leaked through during the extra Input polls
    assert_eq!(line.downstream.poll().unwrap(), None);

    // Now Input resumes, drains Data(2)
    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.work.poll(&mut cx).unwrap();
    let _ = line.output.poll(&mut cx).unwrap();

    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(6))); // (1+2)*2=6
}

#[test]
fn flush_wakes_input_for_remaining_data() {
    let (input_waker, input_woken) = tracking_local_waker();
    let mut line = Line::with_input_waker(mock(), input_waker);
    let mut cx = noop_context();

    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    // Input drains Data(1), hits Flush → Flushing. Data(2) still in the bridge.
    let _ = line.input.poll(&mut cx).unwrap();
    // Work → Ready → FlushReady
    let _ = line.work.poll(&mut cx).unwrap();

    assert!(!input_woken.get());

    // Output forwards Flush → Running. Must wake Input.
    let _ = line.output.poll(&mut cx).unwrap();

    assert!(
        input_woken.get(),
        "Output must wake Input after flush so remaining data is drained"
    );
}

/// Drive skips the routine while a signal waits for Output, swallowing a routine wake that lands
/// then: the return to Running must wake Work to replay it.
#[test]
fn forwarding_a_flush_wakes_work() {
    let mut line = Line::new(mock());
    let mut cx = noop_context();
    line.upstream.push_back(Message::Flush("f".into())).unwrap();
    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.work.poll(&mut cx).unwrap();

    line.work_woken.set(false);
    let _ = line.output.poll(&mut cx).unwrap();
    assert_eq!(line.received(), vec![Message::Flush("f".into())]);
    assert!(line.work_woken.get());
}

#[test]
fn forwarding_a_marker_wakes_work() {
    let mut line = Line::new(mock());
    let mut cx = noop_context();
    line.upstream
        .push_back(Message::Marker("m".into()))
        .unwrap();
    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.work.poll(&mut cx).unwrap();

    line.work_woken.set(false);
    let _ = line.output.poll(&mut cx).unwrap();
    assert_eq!(line.received(), vec![Message::Marker("m".into())]);
    assert!(line.work_woken.get());
}

#[test]
fn input_takes_at_most_budget_then_wakes_itself() {
    let (input_waker, input_woken) = tracking_local_waker();
    let mut line = Line::with_input_waker(mock(), input_waker);
    let mut cx = noop_context();
    for value in 0..BUDGET + 8 {
        line.upstream.push_back(Message::Data(value)).unwrap();
    }

    // One poll takes exactly BUDGET and owes itself the rest.
    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.output.poll(&mut cx).unwrap();
    assert_eq!(line.received().len(), BUDGET);
    assert!(input_woken.replace(false), "a budget stop must wake Input");

    // The rest fits: an empty edge is upstream's to wake, not ours.
    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.output.poll(&mut cx).unwrap();
    assert_eq!(line.received().len(), 8);
    assert!(!input_woken.get());
}

#[test]
fn held_message_goes_before_a_flush_queued_behind_it() {
    let (mut line, input_woken) = bounded(1);
    let mut cx = noop_context();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    line.upstream.push_back(Message::Flush("f".into())).unwrap();

    // 1 fits, 2 is refused and held; the Flush behind it stays unread.
    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.output.poll(&mut cx).unwrap();
    assert_eq!(line.received(), [Message::Data(1)]);

    // Taking 1 out made room: the routine woke Input, which retries 2 before the Flush.
    assert!(input_woken.replace(false));
    assert_eq!(line.cycle(), [Message::Data(2), Message::Flush("f".into())]);
}

#[test]
fn held_message_goes_before_a_marker_queued_behind_it() {
    let (mut line, input_woken) = bounded(1);
    let mut cx = noop_context();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    line.upstream
        .push_back(Message::Marker("m".into()))
        .unwrap();

    let _ = line.input.poll(&mut cx).unwrap();
    let _ = line.output.poll(&mut cx).unwrap();
    assert_eq!(line.received(), [Message::Data(1)]);

    assert!(input_woken.replace(false));
    assert_eq!(
        line.cycle(),
        [Message::Data(2), Message::Marker("m".into())]
    );
}

#[test]
fn spurious_wake_while_full_reads_nothing_past_the_held_message() {
    let (mut line, input_woken) = bounded(1);
    let mut cx = noop_context();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    line.upstream.push_back(Message::Flush("f".into())).unwrap();
    let _ = line.input.poll(&mut cx).unwrap();

    // Woken while still full: the retry is refused, the Flush behind it stays unread, and Input
    // leaves the next wake to the routine.
    let _ = line.input.poll(&mut cx).unwrap();
    assert!(!input_woken.get());
    let _ = line.work.poll(&mut cx).unwrap();
    let _ = line.output.poll(&mut cx).unwrap();
    let mut received = line.received();

    received.extend(line.cycle());
    assert_eq!(
        received,
        [
            Message::Data(1),
            Message::Data(2),
            Message::Flush("f".into())
        ]
    );
}

#[test]
fn refused_messages_are_not_stranded() {
    let (mut line, input_woken) = bounded(1);
    let mut cx = noop_context();
    for value in 0..5 {
        line.upstream.push_back(Message::Data(value)).unwrap();
    }

    // Input runs only when woken, as on the poll thread: each wake must come from the routine.
    let _ = line.input.poll(&mut cx).unwrap();
    let mut received = Vec::new();
    loop {
        let _ = line.output.poll(&mut cx).unwrap();
        received.extend(line.received());
        if !input_woken.replace(false) {
            break;
        }
        let _ = line.input.poll(&mut cx).unwrap();
    }
    assert_eq!(received, (0..5).map(Message::Data).collect::<Vec<_>>());
}

#[test]
fn close_delivers_what_the_routine_still_accepts() {
    let (mut line, input_woken) = bounded(2);
    let mut cx = noop_context();
    for value in 1..=3 {
        line.upstream.push_back(Message::Data(value)).unwrap();
    }
    // 1 and 2 fit, 3 is held. Then upstream closes.
    let _ = line.input.poll(&mut cx).unwrap();
    line.upstream.close().unwrap();

    // Output makes room, so the held message still goes in before Input sees the close.
    let _ = line.output.poll(&mut cx).unwrap();
    assert!(input_woken.get());
    let result = line.input.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

    let result = line.output.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert_eq!(
        line.received(),
        [Message::Data(1), Message::Data(2), Message::Data(3)]
    );
}

/// A close behind a held message waits for the routine's room, which the refusal owes a wake:
/// nothing queued before the close is dropped.
#[test]
fn close_while_holding_waits_for_room() {
    let (mut line, input_woken) = bounded(1);
    let mut cx = noop_context();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    // 2 is held. The close wakes Input, which keeps holding.
    assert!(line.input.poll(&mut cx).unwrap().is_pending());
    line.upstream.close().unwrap();
    assert!(line.input.poll(&mut cx).unwrap().is_pending());

    // Output makes room; the routine wakes Input, which hands 2 over, then sees the close.
    input_woken.set(false);
    let _ = line.output.poll(&mut cx).unwrap();
    assert!(input_woken.get());
    let result = line.input.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    let result = line.output.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert_eq!(line.received(), [Message::Data(1), Message::Data(2)]);
}

/// The close's wake can be spent on the poll that hits the refusal; the routine's wake still
/// comes once it makes room.
#[test]
fn close_before_the_refusal_waits_for_room() {
    let (mut line, input_woken) = bounded(1);
    let mut cx = noop_context();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    line.upstream.close().unwrap();

    assert!(line.input.poll(&mut cx).unwrap().is_pending());
    input_woken.set(false);
    let _ = line.output.poll(&mut cx).unwrap();
    assert!(input_woken.get());
    let result = line.input.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    let result = line.output.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert_eq!(line.received(), [Message::Data(1), Message::Data(2)]);
}

/// Under backpressure a Flush followed by a Close still lands, in order, before the close.
#[test]
fn a_flush_before_a_close_lands_under_backpressure() {
    let downstream = edge::sync::Receiver::bounded(NonZeroUsize::MIN);
    let mut node = Parked::with(|input| Bounded::new(1, input), fanout(downstream.sender()));
    for value in 1..=4 {
        node.upstream.push_back(Message::Data(value)).unwrap();
    }
    node.upstream.push_back(Message::Flush("f".into())).unwrap();
    node.upstream.close().unwrap();

    // Every phase, then the consumer, round by round until downstream closes.
    let mut received = Vec::new();
    let mut cx = noop_context();
    for _ in 0..100 {
        let _ = node.input.poll(&mut cx);
        let _ = node.work.poll(&mut cx);
        let _ = node.output.poll(&mut cx);
        match downstream.poll() {
            Ok(Some(message)) => received.push(message),
            Ok(None) => {}
            Err(_) => break,
        }
    }
    assert_eq!(
        received,
        [
            Message::Data(1),
            Message::Data(2),
            Message::Data(3),
            Message::Data(4),
            Message::Flush("f".into()),
        ]
    );
}

/// While Forwarding, Input offers the routine nothing and Drive doesn't poll it.
#[test]
fn forwarding_holds_input_and_drive() {
    let downstream = edge::sync::Receiver::bounded(NonZeroUsize::MIN);
    let mut counts = None;
    let mut node = Parked::with(
        |input| {
            let routine = Bounded::new(8, input);
            counts = Some((routine.sends.clone(), routine.polls.clone()));
            routine
        },
        fanout(downstream.sender()),
    );
    let Some((sends, polls)) = counts else {
        panic!("routine built");
    };
    // 1 fills downstream, so the Flush is refused: Forwarding.
    node.feed([
        Message::Data(1),
        Message::Flush("f".into()),
        Message::Data(5),
    ]);
    node.emit();
    let (sent, polled) = (sends.get(), polls.get());

    let mut cx = noop_context();
    let _ = node.input.poll(&mut cx).unwrap();
    let _ = node.work.poll(&mut cx).unwrap();
    node.emit();
    assert_eq!((sends.get(), polls.get()), (sent, polled));

    assert_eq!(downstream.poll().unwrap(), Some(Message::Data(1)));
    node.emit();
    assert_eq!(downstream.poll().unwrap(), Some(Message::Flush("f".into())));
    node.feed([]);
    assert_eq!(sends.get(), sent + 1);
}
