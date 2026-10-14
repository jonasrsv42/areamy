use super::{Input, Output, Work};
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
use crate::poll::waker::{self, ThreadLocalWaker};
use crate::poll::{Phase, Pollable};

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
}

impl<R: LineRoutine<usize, usize>> Line<R> {
    fn new(routine: R) -> Self {
        Self::with_input_waker(routine, noop_local_waker())
    }

    fn with_input_waker(routine: R, input_waker: ThreadLocalWaker) -> Self {
        let edge = input::sync::Receiver::new(std::task::Waker::noop().clone());
        let upstream = edge.sender();
        let downstream = edge::sync::Receiver::new();
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
                waker: noop_local_waker(),
            },
        );
        Self {
            input,
            work,
            output,
            upstream,
            downstream,
        }
    }
}

fn mock() -> MockLine {
    MockLine::new(noop_line_wakers())
}

#[test]
fn poll_processes_input_and_produces_output() {
    let mut line = Line::new(mock());
    let mut wkr = noop_waker();

    line.upstream.push_back(Message::Data(2)).unwrap();

    // Input drains edge → Send to routine
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

impl crate::Send<usize> for ClosedErrorRoutine {
    fn send(&mut self, _: usize) -> Result<(), Error> {
        Ok(())
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
