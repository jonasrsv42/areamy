use super::{LeftInput, Output, RightInput, Work};
use crate::DefaultThread;
use crate::Trackable;
use crate::edge;
use crate::error::ErrorKind;
use crate::message::Message;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::node;
use crate::node::biunion::poll::routine::tests::{MockBiunion, noop_biunion_wakers};
use crate::poll::input;
use crate::poll::waker::mock::{noop_waker, tracking_local_waker};
use crate::poll::{Phase, Pollable};
use std::cell::Cell;
use std::rc::Rc;

type Signal = Trackable<&'static str>;
type Inlet = input::sync::Receiver<usize, Signal>;
type Outlet = edge::sync::Sender<usize, Signal>;
type Node =
    node::Node<usize, usize, usize, Signal, DefaultThread, MockBiunion, Inlet, Inlet, Outlet>;
type Flag = Rc<Cell<bool>>;

/// Whether each phase's waker fired.
struct Woken {
    input: Inputs<Flag, Flag>,
    work: Flag,
    output: Flag,
}

impl Woken {
    fn reset(&self) {
        self.input.left.set(false);
        self.input.right.set(false);
        self.work.set(false);
        self.output.set(false);
    }
}

/// A biunion node over [MockBiunion] with the edges into and out of it.
struct Biunion {
    input: Inputs<LeftInput<Node>, RightInput<Node>>,
    work: Work<Node>,
    output: Output<Node>,
    upstream: Inputs<input::sync::Sender<usize, Signal>, input::sync::Sender<usize, Signal>>,
    downstream: edge::sync::Receiver<usize, Signal>,
    woken: Woken,
}

impl Biunion {
    fn new() -> Self {
        let left = input::sync::Receiver::new(std::task::Waker::noop().clone());
        let right = input::sync::Receiver::new(std::task::Waker::noop().clone());
        let upstream = Inputs {
            left: left.sender(),
            right: right.sender(),
        };
        let downstream = edge::sync::Receiver::new();

        let (left_waker, left_woken) = tracking_local_waker();
        let (right_waker, right_woken) = tracking_local_waker();
        let (work_waker, work_woken) = tracking_local_waker();
        let (output_waker, output_woken) = tracking_local_waker();

        let phases = node::Node::phases(
            Inputs {
                left: Phase {
                    target: left,
                    waker: left_waker,
                },
                right: Phase {
                    target: right,
                    waker: right_waker,
                },
            },
            Phase {
                target: MockBiunion::new(noop_biunion_wakers()),
                waker: work_waker,
            },
            Phase {
                target: downstream.sender(),
                waker: output_waker,
            },
        );

        Self {
            input: phases.inputs,
            work: phases.work,
            output: phases.output,
            upstream,
            downstream,
            woken: Woken {
                input: Inputs {
                    left: left_woken,
                    right: right_woken,
                },
                work: work_woken,
                output: output_woken,
            },
        }
    }
}

// --- Data flow ---

#[test]
fn left_input_processes_data() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream.left.push_back(Message::Data(5)).unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(10))); // 5 * 2
}

#[test]
fn right_input_processes_data() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream.right.push_back(Message::Data(5)).unwrap();
    let _ = b.input.right.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(15))); // 5 * 3
}

#[test]
fn both_inputs_process_data() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream.left.push_back(Message::Data(2)).unwrap();
    b.upstream.right.push_back(Message::Data(3)).unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    let _ = b.input.right.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(4))); // 2 * 2
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(9))); // 3 * 3
}

// --- Signal forwarding ---

#[test]
fn flush_from_left_forwards() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Flush("s1".into()))
    );
}

#[test]
fn flush_from_right_forwards() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .right
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Flush("s1".into()))
    );
}

#[test]
fn marker_from_left_forwards() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Marker("m1".into()))
    );
}

#[test]
fn marker_from_right_forwards() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .right
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Marker("m1".into()))
    );
}

// --- Close propagation ---

#[test]
fn close_from_left_propagates() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut wkr);
    assert!(matches!(
        b.input.right.poll(&mut wkr).unwrap_err().kind,
        ErrorKind::Closed
    ));
    let _ = b.work.poll(&mut wkr);
    assert!(matches!(
        b.output.poll(&mut wkr).unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn data_then_close_drains_before_closing() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream.left.push_back(Message::Data(7)).unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(14))); // 7 * 2

    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut wkr);
    let _ = b.work.poll(&mut wkr);
    assert!(matches!(
        b.output.poll(&mut wkr).unwrap_err().kind,
        ErrorKind::Closed
    ));
}

// --- Input waker: close wakes other input ---

#[test]
fn close_from_left_wakes_right() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut wkr);
    assert!(
        b.woken.input.right.get(),
        "closing left must wake right input"
    );
}

#[test]
fn close_from_right_wakes_left() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream.right.close().unwrap();
    let _ = b.input.right.poll(&mut wkr);
    assert!(
        b.woken.input.left.get(),
        "closing right must wake left input"
    );
}

// --- Input waker: close/flush/marker wakes work ---

#[test]
fn close_from_left_wakes_work() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut wkr);
    assert!(b.woken.work.get(), "closing left must wake work phase");
}

#[test]
fn close_from_right_wakes_work() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream.right.close().unwrap();
    let _ = b.input.right.poll(&mut wkr);
    assert!(b.woken.work.get(), "closing right must wake work phase");
}

#[test]
fn flush_from_left_wakes_work() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    assert!(b.woken.work.get(), "flush from left must wake work phase");
}

#[test]
fn flush_from_right_wakes_work() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream
        .right
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut wkr).unwrap();
    assert!(b.woken.work.get(), "flush from right must wake work phase");
}

#[test]
fn marker_from_left_wakes_work() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    assert!(b.woken.work.get(), "marker from left must wake work phase");
}

#[test]
fn marker_from_right_wakes_work() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.woken.reset();
    b.upstream
        .right
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut wkr).unwrap();
    assert!(b.woken.work.get(), "marker from right must wake work phase");
}

// --- Work waker: flush/close/marker ready wakes output ---

#[test]
fn flush_ready_wakes_output() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    b.woken.reset();
    let _ = b.work.poll(&mut wkr).unwrap();
    assert!(
        b.woken.output.get(),
        "work must wake output when flush ready"
    );
}

#[test]
fn close_wakes_output_directly() {
    // Fast close: drain_left transitions straight to CloseReady and
    // wakes Output directly (no flush, no work polling).
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream.left.close().unwrap();
    b.woken.reset();
    let _ = b.input.left.poll(&mut wkr);
    assert!(b.woken.output.get(), "drain_left must wake output on close");
}

#[test]
fn marker_ready_wakes_output() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    b.woken.reset();
    let _ = b.work.poll(&mut wkr).unwrap();
    assert!(
        b.woken.output.get(),
        "work must wake output when marker ready"
    );
}

// --- Output waker: flush/marker forwarded wakes both inputs ---

#[test]
fn flush_forwarded_wakes_both_inputs() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    b.woken.reset();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert!(
        b.woken.input.left.get(),
        "output must wake left after flush forwarded"
    );
    assert!(
        b.woken.input.right.get(),
        "output must wake right after flush forwarded"
    );
}

#[test]
fn marker_forwarded_wakes_both_inputs() {
    let mut b = Biunion::new();
    let mut wkr = noop_waker();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut wkr).unwrap();
    let _ = b.work.poll(&mut wkr).unwrap();
    b.woken.reset();
    let _ = b.output.poll(&mut wkr).unwrap();
    assert!(
        b.woken.input.left.get(),
        "output must wake left after marker forwarded"
    );
    assert!(
        b.woken.input.right.get(),
        "output must wake right after marker forwarded"
    );
}
