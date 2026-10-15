use super::{Input, Output, Work};
use crate::Accept;
use crate::DefaultThread;
use crate::Trackable;
use crate::edge;
use crate::error::{Error, ErrorKind};
use crate::message::Message;
use crate::node::line::poll::node;
use crate::node::line::poll::routine::LineRoutine;
use crate::node::line::poll::routine::tests::{MockLine, noop_line_wakers};
use crate::poll::input;
use crate::poll::waker::mock::{noop_local_waker, noop_waker, tracking_local_waker};
use crate::poll::waker::{self, LocalWaker};
use crate::poll::{BUDGET, Phase, Pollable};
use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;

type Signal = Trackable<&'static str>;
type Inlet = input::sync::Receiver<usize, Signal>;
type Outlet = edge::sync::Sender<usize, Signal>;
type Node<R> = node::Node<usize, usize, Signal, DefaultThread, R, Inlet, Outlet>;

/// A line node with the edges into and out of it.
struct Line<R: LineRoutine<usize, usize>> {
    input: Input<Node<R>>,
    work: Work<Node<R>>,
    output: Output<Node<R>>,
    upstream: input::sync::Sender<usize, Signal>,
    downstream: edge::sync::Receiver<usize, Signal>,
    /// Whether Output's waker fired.
    output_woken: Rc<Cell<bool>>,
}

impl<R: LineRoutine<usize, usize>> Line<R> {
    fn new(routine: R) -> Self {
        Self::with_input_waker(routine, noop_local_waker())
    }

    fn with_input_waker(routine: R, input_waker: LocalWaker) -> Self {
        let edge = input::sync::Receiver::new(std::task::Waker::noop().clone());
        let upstream = edge.sender();
        let downstream = edge::sync::Receiver::new();
        let (output_waker, output_woken) = tracking_local_waker();
        let (input, work, output) = node::Node::phases(
            Phase {
                target: edge,
                waker: input_waker,
            },
            Phase {
                target: routine,
                waker: noop_local_waker(),
            },
            Phase {
                target: downstream.sender(),
                waker: output_waker,
            },
        );
        Self {
            input,
            work,
            output,
            upstream,
            downstream,
            output_woken,
        }
    }

    /// Everything that reached downstream so far.
    fn received(&self) -> Vec<Message<usize, Signal>> {
        let mut received = Vec::new();
        loop {
            match self.downstream.poll() {
                Ok(Some(message)) => received.push(message),
                Ok(None) => return received,
                Err(e) if matches!(e.kind, ErrorKind::Closed) => return received,
                Err(e) => panic!("downstream failed: {e:?}"),
            }
        }
    }

    /// Poll Input, Work and Output once each; what reached downstream.
    fn cycle(&mut self) -> Vec<Message<usize, Signal>> {
        let mut wkr = noop_waker();
        let _ = self.input.poll(&mut wkr).unwrap();
        let _ = self.work.poll(&mut wkr).unwrap();
        let _ = self.output.poll(&mut wkr).unwrap();
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
}

impl Bounded {
    fn new(room: usize, input: LocalWaker) -> Self {
        Self {
            queue: VecDeque::new(),
            room,
            refused: false,
            input,
            flushed: false,
        }
    }
}

impl crate::TrySend<usize> for Bounded {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
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
        if message.is_some() && std::mem::take(&mut self.refused) {
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
    fn poll(&mut self, _waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        Ok(match std::mem::take(&mut self.flushed) {
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
    let mut wkr = noop_waker();

    line.upstream.push_back(Message::Data(2)).unwrap();

    // Input drains edge → TrySend to routine
    assert!(line.input.poll(&mut wkr).unwrap().is_pending());
    // Work polls routine (sync mock — no-op here)
    assert!(line.work.poll(&mut wkr).unwrap().is_pending());
    // Output drains Next → pushes downstream
    assert!(line.output.poll(&mut wkr).unwrap().is_pending());

    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(4)));
}

#[test]
fn poll_returns_closed_on_closed_input() {
    let mut line = Line::new(mock());
    let mut wkr = noop_waker();

    line.upstream.close().unwrap();

    // Fast close: Input detects closed → CloseReady directly,
    // skipping the Closing/poll-routine cycle. Work is irrelevant.
    let result = line.input.poll(&mut wkr);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert!(line.output_woken.get(), "Input must wake Output on close");

    // Output drains buffered output (none here) and closes downstream.
    let result = line.output.poll(&mut wkr);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
}

#[test]
fn poll_calls_routine_poll() {
    let mut line = Line::new(mock());
    let mut wkr = noop_waker();

    let _ = line.work.poll(&mut wkr).unwrap();
    assert_eq!(line.work.shared.borrow().work.target.poll_count, 1);

    let _ = line.work.poll(&mut wkr).unwrap();
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
    fn poll(&mut self, _waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        Err(crate::closed!())
    }
}

impl LineRoutine<usize, usize> for ClosedErrorRoutine {}

#[test]
fn routine_returning_closed_becomes_fatal() {
    let mut line = Line::new(ClosedErrorRoutine);
    let mut wkr = noop_waker();

    let err = line.work.poll(&mut wkr).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn routine_returning_closed_from_try_send_becomes_fatal() {
    let mut line = Line::new(ClosedErrorRoutine);
    let mut wkr = noop_waker();

    line.upstream.push_back(Message::Data(1)).unwrap();

    let err = line.input.poll(&mut wkr).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn routine_returning_closed_during_flush_becomes_fatal() {
    let mut line = Line::new(ClosedErrorRoutine);
    let mut wkr = noop_waker();

    line.upstream
        .push_back(Message::Flush("s1".into()))
        .unwrap();

    let _ = line.input.poll(&mut wkr);

    let err = line.work.poll(&mut wkr).unwrap_err();
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
    fn poll(&mut self, _waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        Ok(core::task::Poll::Ready(()))
    }
}

impl LineRoutine<usize, usize> for ReadyRoutine {}

#[test]
fn routine_ready_while_running_is_fatal() {
    let mut line = Line::new(ReadyRoutine);
    let mut wkr = noop_waker();

    let err = line.work.poll(&mut wkr).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn routine_ready_on_a_marker_is_fatal() {
    let mut line = Line::new(ReadyRoutine);
    let mut wkr = noop_waker();

    line.upstream
        .push_back(Message::Marker("m".into()))
        .unwrap();
    let _ = line.input.poll(&mut wkr);

    let err = line.work.poll(&mut wkr).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn poll_drains_multiple_inputs() {
    let mut line = Line::new(mock());
    let mut wkr = noop_waker();

    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    let _ = line.input.poll(&mut wkr).unwrap();
    let _ = line.work.poll(&mut wkr).unwrap();
    let _ = line.output.poll(&mut wkr).unwrap();

    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(2)));
    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(6)));
}

#[test]
fn marker_preserves_ordering_with_data() {
    let mut line = Line::new(mock());
    let mut wkr = noop_waker();

    // Data then Marker then more Data
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    // First Input poll: drains Data(1), hits Marker → MarkerReady, stops
    let _ = line.input.poll(&mut wkr).unwrap();

    // Poll Input again — should be blocked in MarkerReady, Data(2) stays in edge
    let _ = line.input.poll(&mut wkr).unwrap();
    let _ = line.input.poll(&mut wkr).unwrap();

    let _ = line.work.poll(&mut wkr).unwrap();

    // Output drains data before marker, then forwards marker → Running
    let _ = line.output.poll(&mut wkr).unwrap();

    // Data(1) output arrives before Marker — ordering preserved
    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(2))); // 1*2=2
    assert_eq!(
        line.downstream.poll().unwrap(),
        Some(Message::Marker("m1".into()))
    );
    // Data(2) must NOT have leaked through during the extra Input polls
    assert_eq!(line.downstream.poll().unwrap(), None);

    // Now Input resumes, drains Data(2)
    let _ = line.input.poll(&mut wkr).unwrap();
    let _ = line.work.poll(&mut wkr).unwrap();
    let _ = line.output.poll(&mut wkr).unwrap();

    assert_eq!(line.downstream.poll().unwrap(), Some(Message::Data(6))); // (1+2)*2=6
}

#[test]
fn flush_wakes_input_for_remaining_data() {
    let (input_waker, input_woken) = tracking_local_waker();
    let mut line = Line::with_input_waker(mock(), input_waker);
    let mut wkr = noop_waker();

    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    // Input drains Data(1), hits Flush → Flushing. Data(2) still in the bridge.
    let _ = line.input.poll(&mut wkr).unwrap();
    // Work → Ready → FlushReady
    let _ = line.work.poll(&mut wkr).unwrap();

    assert!(!input_woken.get());

    // Output forwards Flush → Running. Must wake Input.
    let _ = line.output.poll(&mut wkr).unwrap();

    assert!(
        input_woken.get(),
        "Output must wake Input after flush so remaining data is drained"
    );
}

#[test]
fn input_takes_at_most_budget_then_wakes_itself() {
    let (input_waker, input_woken) = tracking_local_waker();
    let mut line = Line::with_input_waker(mock(), input_waker);
    let mut wkr = noop_waker();
    for value in 0..BUDGET + 8 {
        line.upstream.push_back(Message::Data(value)).unwrap();
    }

    // One poll takes exactly BUDGET and owes itself the rest.
    let _ = line.input.poll(&mut wkr).unwrap();
    let _ = line.output.poll(&mut wkr).unwrap();
    assert_eq!(line.received().len(), BUDGET);
    assert!(input_woken.replace(false), "a budget stop must wake Input");

    // The rest fits: an empty edge is upstream's to wake, not ours.
    let _ = line.input.poll(&mut wkr).unwrap();
    let _ = line.output.poll(&mut wkr).unwrap();
    assert_eq!(line.received().len(), 8);
    assert!(!input_woken.get());
}

#[test]
fn held_message_goes_before_a_flush_queued_behind_it() {
    let (mut line, input_woken) = bounded(1);
    let mut wkr = noop_waker();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    line.upstream.push_back(Message::Flush("f".into())).unwrap();

    // 1 fits, 2 is refused and held; the Flush behind it stays unread.
    let _ = line.input.poll(&mut wkr).unwrap();
    let _ = line.output.poll(&mut wkr).unwrap();
    assert_eq!(line.received(), [Message::Data(1)]);

    // Taking 1 out made room: the routine woke Input, which retries 2 before the Flush.
    assert!(input_woken.replace(false));
    assert_eq!(line.cycle(), [Message::Data(2), Message::Flush("f".into())]);
}

#[test]
fn held_message_goes_before_a_marker_queued_behind_it() {
    let (mut line, input_woken) = bounded(1);
    let mut wkr = noop_waker();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    line.upstream
        .push_back(Message::Marker("m".into()))
        .unwrap();

    let _ = line.input.poll(&mut wkr).unwrap();
    let _ = line.output.poll(&mut wkr).unwrap();
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
    let mut wkr = noop_waker();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    line.upstream.push_back(Message::Flush("f".into())).unwrap();
    let _ = line.input.poll(&mut wkr).unwrap();

    // Woken while still full: the retry is refused, the Flush behind it stays unread, and Input
    // leaves the next wake to the routine.
    let _ = line.input.poll(&mut wkr).unwrap();
    assert!(!input_woken.get());
    let _ = line.work.poll(&mut wkr).unwrap();
    let _ = line.output.poll(&mut wkr).unwrap();
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
    let mut wkr = noop_waker();
    for value in 0..5 {
        line.upstream.push_back(Message::Data(value)).unwrap();
    }

    // Input runs only when woken, as on the poll thread: each wake must come from the routine.
    let _ = line.input.poll(&mut wkr).unwrap();
    let mut received = Vec::new();
    loop {
        let _ = line.output.poll(&mut wkr).unwrap();
        received.extend(line.received());
        if !input_woken.replace(false) {
            break;
        }
        let _ = line.input.poll(&mut wkr).unwrap();
    }
    assert_eq!(received, (0..5).map(Message::Data).collect::<Vec<_>>());
}

#[test]
fn close_delivers_what_the_routine_still_accepts() {
    let (mut line, input_woken) = bounded(2);
    let mut wkr = noop_waker();
    for value in 1..=3 {
        line.upstream.push_back(Message::Data(value)).unwrap();
    }
    // 1 and 2 fit, 3 is held. Then upstream closes.
    let _ = line.input.poll(&mut wkr).unwrap();
    line.upstream.close().unwrap();

    // Output makes room, so the held message still goes in before Input sees the close.
    let _ = line.output.poll(&mut wkr).unwrap();
    assert!(input_woken.get());
    let result = line.input.poll(&mut wkr);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

    let result = line.output.poll(&mut wkr);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert_eq!(
        line.received(),
        [Message::Data(1), Message::Data(2), Message::Data(3)]
    );
}

#[test]
fn close_while_holding_closes_at_once() {
    let (mut line, _) = bounded(1);
    let mut wkr = noop_waker();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();

    // 2 is held, and the routine never makes room.
    assert!(line.input.poll(&mut wkr).unwrap().is_pending());

    // Close wakes Input; the held message can't be waited out, and Output must hear.
    line.upstream.close().unwrap();
    let result = line.input.poll(&mut wkr);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert!(line.output_woken.get());
}

#[test]
fn close_before_the_refusal_closes_at_once() {
    let (mut line, _) = bounded(1);
    let mut wkr = noop_waker();
    line.upstream.push_back(Message::Data(1)).unwrap();
    line.upstream.push_back(Message::Data(2)).unwrap();
    // The close's wake is spent on the poll that then hits the refusal, so no later wake will
    // come: that poll itself must see the close.
    line.upstream.close().unwrap();

    let result = line.input.poll(&mut wkr);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert!(line.output_woken.get());
}
