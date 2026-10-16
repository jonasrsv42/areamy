use super::{Input, Output, Work};
use crate::Trackable;
use crate::biunion::{self, Left, Right};
use crate::edge;
use crate::error::{Error, ErrorKind};
use crate::message::Message;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::node;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::node::biunion::poll::routine::tests::{MockBiunion, noop_biunion_wakers};
use crate::poll::context::mock::noop_context;
use crate::poll::input;
use crate::poll::{BUDGET, BiunionInputs, Context, Phase, Pollable};
use crate::wake::Waker;
use crate::wake::mock::{counting_waker, tracking_local_waker};
use crate::{Accept, DefaultThread};
use std::cell::Cell;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

type Signal = Trackable<&'static str>;
type Inlet = input::sync::Receiver<usize, Signal>;
type Outlet = edge::sync::Sender<usize, Signal>;
type Node<R> = node::Node<usize, usize, usize, Signal, DefaultThread, R, Inlet, Inlet, Outlet>;
type Flag = Rc<Cell<bool>>;

/// A routine these tests can run a node over.
trait Routine: BiunionRoutine<usize, usize, usize> + 'static {}
impl<R: BiunionRoutine<usize, usize, usize> + 'static> Routine for R {}

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

/// A biunion node with the edges into and out of it.
struct Biunion<R: Routine> {
    input: Inputs<Input<Node<R>, Left>, Input<Node<R>, Right>>,
    work: Work<Node<R>>,
    output: Output<Node<R>>,
    upstream: Inputs<input::sync::Sender<usize, Signal>, input::sync::Sender<usize, Signal>>,
    downstream: edge::sync::Receiver<usize, Signal>,
    woken: Woken,
    /// Output woken by its edges.
    output_wakes: Arc<AtomicUsize>,
}

impl Biunion<MockBiunion> {
    fn new() -> Self {
        Self::with(|_| MockBiunion::new(noop_biunion_wakers()))
    }
}

impl<R: Routine> Biunion<R> {
    /// A node over the routine `routine` builds from the input wakers.
    fn with(routine: impl FnOnce(BiunionInputs) -> R) -> Self {
        Self::draining_into(routine, edge::sync::Receiver::new())
    }

    /// Same, draining into `downstream`.
    fn draining_into(
        routine: impl FnOnce(BiunionInputs) -> R,
        downstream: edge::sync::Receiver<usize, Signal>,
    ) -> Self {
        let left = input::sync::Receiver::new(std::task::Waker::noop().clone(), None);
        let right = input::sync::Receiver::new(std::task::Waker::noop().clone(), None);
        let upstream = Inputs {
            left: left.sender(),
            right: right.sender(),
        };

        let (left_waker, left_woken) = tracking_local_waker();
        let (right_waker, right_woken) = tracking_local_waker();
        let (work_waker, work_woken) = tracking_local_waker();
        let (output_waker, output_woken) = tracking_local_waker();
        // The edges wake Output through the sync half.
        let (output_wake, output_wakes) = counting_waker();
        let output_sync = output_wake.sync;
        let routine = routine(Inputs {
            left: left_waker.clone(),
            right: right_waker.clone(),
        });

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
                target: routine,
                waker: work_waker,
            },
            downstream.sender().into(),
            Waker {
                sync: output_sync,
                local: output_waker,
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
            output_wakes,
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

    /// Poll both Inputs, Work and Output once each; what reached downstream.
    fn cycle(&mut self) -> Vec<Message<usize, Signal>> {
        let mut cx = noop_context();
        let _ = self.input.left.poll(&mut cx).unwrap();
        let _ = self.input.right.poll(&mut cx).unwrap();
        let _ = self.work.poll(&mut cx).unwrap();
        let _ = self.output.poll(&mut cx).unwrap();
        self.received()
    }
}

/// How many messages [Bounded] holds before it refuses.
enum Room {
    /// Each side its own budget.
    Sides(Inputs<usize, usize>),
    /// One budget for both sides.
    Shared(usize),
}

/// Refuses past its [Room]; once [Next] frees room, wakes each refused side that room was for,
/// as the [TrySend](crate::TrySend) contract requires. Left output is ×2, right ×3, so a
/// mixed-up side shows.
struct Bounded {
    queue: VecDeque<(bool, usize)>,
    room: Room,
    refused: Inputs<bool, bool>,
    wakers: BiunionInputs,
    flushed: bool,
}

impl Bounded {
    fn new(room: Room, wakers: BiunionInputs) -> Self {
        Self {
            queue: VecDeque::new(),
            room,
            refused: Inputs {
                left: false,
                right: false,
            },
            wakers,
            flushed: false,
        }
    }

    fn full(&self, left: bool) -> bool {
        match &self.room {
            Room::Shared(room) => self.queue.len() >= *room,
            Room::Sides(room) => {
                let room = if left { room.left } else { room.right };
                self.queue.iter().filter(|(side, _)| *side == left).count() >= room
            }
        }
    }
}

impl crate::TrySend<usize, biunion::Left> for Bounded {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
        if self.full(true) {
            self.refused.left = true;
            return Ok(Accept::Full(message));
        }
        self.queue.push_back((true, message * 2));
        Ok(Accept::Accepted)
    }
}

impl crate::TrySend<usize, biunion::Right> for Bounded {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
        if self.full(false) {
            self.refused.right = true;
            return Ok(Accept::Full(message));
        }
        self.queue.push_back((false, message * 3));
        Ok(Accept::Accepted)
    }
}

impl crate::Next<usize> for Bounded {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        let Some((left, value)) = self.queue.pop_front() else {
            return Ok(None);
        };
        // Room freed: one budget frees room for both sides, a per-side one for its own.
        let shared = matches!(self.room, Room::Shared(_));
        if (shared || left) && std::mem::take(&mut self.refused.left) {
            self.wakers.left.wake();
        }
        if (shared || !left) && std::mem::take(&mut self.refused.right) {
            self.wakers.right.wake();
        }
        Ok(Some(value))
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
        Ok(match std::mem::take(&mut self.flushed) {
            true => core::task::Poll::Ready(()),
            false => core::task::Poll::Pending,
        })
    }
}

impl BiunionRoutine<usize, usize, usize> for Bounded {}

/// Returns `Closed` from every `try_send`.
struct ClosedTrySend;

impl crate::TrySend<usize, biunion::Left> for ClosedTrySend {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        Err(crate::closed!())
    }
}

impl crate::TrySend<usize, biunion::Right> for ClosedTrySend {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        Err(crate::closed!())
    }
}

impl crate::Next<usize> for ClosedTrySend {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl crate::Flush for ClosedTrySend {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::Poll for ClosedTrySend {
    fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        Ok(core::task::Poll::Pending)
    }
}

impl BiunionRoutine<usize, usize, usize> for ClosedTrySend {}

/// One side of [Biunion], so each test body runs for both.
trait TestSide {
    /// The opposite side.
    type Other: TestSide;
    /// What both mocks, [Bounded] and [MockBiunion], multiply this side's data by.
    const MUL: usize;
    /// `room` on this side, unbounded on the other.
    fn room(room: usize) -> Inputs<usize, usize>;
    fn upstream<R: Routine>(b: &Biunion<R>) -> &input::sync::Sender<usize, Signal>;
    fn poll<R: Routine>(b: &mut Biunion<R>) -> Result<core::task::Poll<()>, Error>;
    fn woken<R: Routine>(b: &Biunion<R>) -> &Flag;
}

impl TestSide for Left {
    type Other = Right;
    const MUL: usize = 2;
    fn room(room: usize) -> Inputs<usize, usize> {
        Inputs {
            left: room,
            right: usize::MAX,
        }
    }
    fn upstream<R: Routine>(b: &Biunion<R>) -> &input::sync::Sender<usize, Signal> {
        &b.upstream.left
    }
    fn poll<R: Routine>(b: &mut Biunion<R>) -> Result<core::task::Poll<()>, Error> {
        b.input.left.poll(&mut noop_context())
    }
    fn woken<R: Routine>(b: &Biunion<R>) -> &Flag {
        &b.woken.input.left
    }
}

impl TestSide for Right {
    type Other = Left;
    const MUL: usize = 3;
    fn room(room: usize) -> Inputs<usize, usize> {
        Inputs {
            left: usize::MAX,
            right: room,
        }
    }
    fn upstream<R: Routine>(b: &Biunion<R>) -> &input::sync::Sender<usize, Signal> {
        &b.upstream.right
    }
    fn poll<R: Routine>(b: &mut Biunion<R>) -> Result<core::task::Poll<()>, Error> {
        b.input.right.poll(&mut noop_context())
    }
    fn woken<R: Routine>(b: &Biunion<R>) -> &Flag {
        &b.woken.input.right
    }
}

/// A node over [Bounded] with `room` on side `S` only.
fn bounded<S: TestSide>(room: usize) -> Biunion<Bounded> {
    Biunion::with(|wakers| Bounded::new(Room::Sides(S::room(room)), wakers))
}

/// As [bounded], but the routine can't wake the sides: any wake they see is the node's.
fn bounded_mute<S: TestSide>(room: usize) -> Biunion<Bounded> {
    Biunion::with(|_| Bounded::new(Room::Sides(S::room(room)), noop_biunion_wakers().input))
}

fn data<S: TestSide>(values: impl IntoIterator<Item = usize>) -> Vec<Message<usize, Signal>> {
    values
        .into_iter()
        .map(|value| Message::Data(value * S::MUL))
        .collect()
}

// --- Data flow ---

#[test]
fn left_input_processes_data() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream.left.push_back(Message::Data(5)).unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(10))); // 5 * 2
}

#[test]
fn right_input_processes_data() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream.right.push_back(Message::Data(5)).unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(15))); // 5 * 3
}

#[test]
fn both_inputs_process_data() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream.left.push_back(Message::Data(2)).unwrap();
    b.upstream.right.push_back(Message::Data(3)).unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(4))); // 2 * 2
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(9))); // 3 * 3
}

/// Output parks on a full edge, bound to its own waker: the pop wakes it, and the held message
/// goes out first.
#[test]
fn output_parks_on_a_full_edge_and_resumes_on_the_pop() {
    let downstream = edge::sync::Receiver::bounded(NonZeroUsize::MIN);
    let mut b = Biunion::draining_into(|_| MockBiunion::new(noop_biunion_wakers()), downstream);
    let mut cx = noop_context();
    b.upstream.left.push_back(Message::Data(2)).unwrap();
    b.upstream.right.push_back(Message::Data(3)).unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    // 4 fills the edge; 9 is held.
    assert!(b.output.poll(&mut cx).unwrap().is_pending());
    assert_eq!(b.output_wakes.load(Ordering::SeqCst), 0);

    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(4)));
    assert_eq!(b.output_wakes.load(Ordering::SeqCst), 1);
    assert!(b.output.poll(&mut cx).unwrap().is_pending());
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(9)));
}

// --- Signal forwarding ---

#[test]
fn flush_from_left_forwards() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Flush("s1".into()))
    );
}

#[test]
fn flush_from_right_forwards() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .right
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Flush("s1".into()))
    );
}

#[test]
fn marker_from_left_forwards() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Marker("m1".into()))
    );
}

#[test]
fn marker_from_right_forwards() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .right
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(
        b.downstream.poll().unwrap(),
        Some(Message::Marker("m1".into()))
    );
}

// --- Close propagation ---

#[test]
fn close_from_left_propagates() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut cx);
    assert!(matches!(
        b.input.right.poll(&mut cx).unwrap_err().kind,
        ErrorKind::Closed
    ));
    let _ = b.work.poll(&mut cx);
    assert!(matches!(
        b.output.poll(&mut cx).unwrap_err().kind,
        ErrorKind::Closed
    ));
}

#[test]
fn data_then_close_drains_before_closing() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream.left.push_back(Message::Data(7)).unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.downstream.poll().unwrap(), Some(Message::Data(14))); // 7 * 2

    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut cx);
    let _ = b.work.poll(&mut cx);
    assert!(matches!(
        b.output.poll(&mut cx).unwrap_err().kind,
        ErrorKind::Closed
    ));
}

// --- Closed outlet: Output's close error tears the whole node down ---

impl<R: Routine> Biunion<R> {
    /// Drop the downstream receiver, then poll Output into the closed outlet: every other phase
    /// is woken and reports Closed.
    fn close_downstream_under_output(self) {
        let Self {
            mut input,
            mut work,
            mut output,
            downstream,
            woken,
            ..
        } = self;
        let mut cx = noop_context();
        drop(downstream);
        woken.reset();

        assert!(matches!(
            output.poll(&mut cx).unwrap_err().kind,
            ErrorKind::Closed
        ));
        assert!(woken.input.left.get(), "left input must be woken");
        assert!(woken.input.right.get(), "right input must be woken");
        assert!(woken.work.get(), "work must be woken");
        for (phase, polled) in [
            ("left", input.left.poll(&mut cx)),
            ("right", input.right.poll(&mut cx)),
            ("work", work.poll(&mut cx)),
        ] {
            let kind = polled.map_err(|e| e.kind);
            assert!(
                matches!(kind, Err(ErrorKind::Closed)),
                "{phase} must report Closed, got {kind:?}"
            );
        }
    }
}

#[test]
fn closed_outlet_while_running_closes_the_node() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream.left.push_back(Message::Data(7)).unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    b.close_downstream_under_output();
}

#[test]
fn closed_outlet_on_a_flush_closes_the_node() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    b.close_downstream_under_output();
}

#[test]
fn closed_outlet_on_a_marker_closes_the_node() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .right
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    b.close_downstream_under_output();
}

// --- Input waker: close wakes other input ---

#[test]
fn close_from_left_wakes_right() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut cx);
    assert!(
        b.woken.input.right.get(),
        "closing left must wake right input"
    );
}

#[test]
fn close_from_right_wakes_left() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream.right.close().unwrap();
    let _ = b.input.right.poll(&mut cx);
    assert!(
        b.woken.input.left.get(),
        "closing right must wake left input"
    );
}

// --- Input waker: close/flush/marker wakes work ---

#[test]
fn close_from_left_wakes_work() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream.left.close().unwrap();
    let _ = b.input.left.poll(&mut cx);
    assert!(b.woken.work.get(), "closing left must wake work phase");
}

#[test]
fn close_from_right_wakes_work() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream.right.close().unwrap();
    let _ = b.input.right.poll(&mut cx);
    assert!(b.woken.work.get(), "closing right must wake work phase");
}

#[test]
fn flush_from_left_wakes_work() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    assert!(b.woken.work.get(), "flush from left must wake work phase");
}

#[test]
fn flush_from_right_wakes_work() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream
        .right
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    assert!(b.woken.work.get(), "flush from right must wake work phase");
}

#[test]
fn marker_from_left_wakes_work() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    assert!(b.woken.work.get(), "marker from left must wake work phase");
}

#[test]
fn marker_from_right_wakes_work() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.woken.reset();
    b.upstream
        .right
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();
    assert!(b.woken.work.get(), "marker from right must wake work phase");
}

// --- Work waker: flush/close/marker ready wakes output ---

#[test]
fn flush_ready_wakes_output() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    b.woken.reset();
    let _ = b.work.poll(&mut cx).unwrap();
    assert!(
        b.woken.output.get(),
        "work must wake output when flush ready"
    );
}

#[test]
fn close_wakes_output_directly() {
    // Fast close: Input transitions straight to CloseReady and
    // wakes Output directly (no flush, no work polling).
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream.left.close().unwrap();
    b.woken.reset();
    let _ = b.input.left.poll(&mut cx);
    assert!(b.woken.output.get(), "left input must wake output on close");
}

#[test]
fn marker_ready_wakes_output() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    b.woken.reset();
    let _ = b.work.poll(&mut cx).unwrap();
    assert!(
        b.woken.output.get(),
        "work must wake output when marker ready"
    );
}

// --- Output waker: flush/marker forwarded wakes both inputs ---

#[test]
fn flush_forwarded_wakes_both_inputs() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    b.upstream
        .left
        .push_back(Message::Flush("s1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    b.woken.reset();
    let _ = b.output.poll(&mut cx).unwrap();
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
    let mut cx = noop_context();
    b.upstream
        .left
        .push_back(Message::Marker("m1".into()))
        .unwrap();
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.work.poll(&mut cx).unwrap();
    b.woken.reset();
    let _ = b.output.poll(&mut cx).unwrap();
    assert!(
        b.woken.input.left.get(),
        "output must wake left after marker forwarded"
    );
    assert!(
        b.woken.input.right.get(),
        "output must wake right after marker forwarded"
    );
}

// --- Backpressure, per side: each body runs for Left and for Right ---

fn held_goes_before_a_flush_queued_behind_it<S: TestSide>() {
    let mut b = bounded::<S>(1);
    let mut cx = noop_context();
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    S::upstream(&b)
        .push_back(Message::Flush("f".into()))
        .unwrap();

    // 1 fits, 2 is refused and held; the Flush behind it stays unread.
    let _ = S::poll(&mut b).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.received(), data::<S>([1]));

    // Taking 1 out made room: the routine woke this side, which retries 2 before the Flush.
    assert!(S::woken(&b).replace(false));
    let mut expected = data::<S>([2]);
    expected.push(Message::Flush("f".into()));
    assert_eq!(b.cycle(), expected);
}

#[test]
fn left_held_goes_before_a_flush_queued_behind_it() {
    held_goes_before_a_flush_queued_behind_it::<Left>();
}

#[test]
fn right_held_goes_before_a_flush_queued_behind_it() {
    held_goes_before_a_flush_queued_behind_it::<Right>();
}

fn held_goes_before_a_marker_queued_behind_it<S: TestSide>() {
    let mut b = bounded::<S>(1);
    let mut cx = noop_context();
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    S::upstream(&b)
        .push_back(Message::Marker("m".into()))
        .unwrap();

    let _ = S::poll(&mut b).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.received(), data::<S>([1]));

    assert!(S::woken(&b).replace(false));
    let mut expected = data::<S>([2]);
    expected.push(Message::Marker("m".into()));
    assert_eq!(b.cycle(), expected);
}

#[test]
fn left_held_goes_before_a_marker_queued_behind_it() {
    held_goes_before_a_marker_queued_behind_it::<Left>();
}

#[test]
fn right_held_goes_before_a_marker_queued_behind_it() {
    held_goes_before_a_marker_queued_behind_it::<Right>();
}

fn spurious_wake_while_full_reads_nothing_past_the_held_message<S: TestSide>() {
    let mut b = bounded::<S>(1);
    let mut cx = noop_context();
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    S::upstream(&b)
        .push_back(Message::Flush("f".into()))
        .unwrap();
    let _ = S::poll(&mut b).unwrap();
    assert!(!S::woken(&b).replace(false));

    // Woken while still full: the retry is refused, the Flush behind it stays unread, and the
    // side leaves the next wake to the routine.
    let _ = S::poll(&mut b).unwrap();
    assert!(!S::woken(&b).get());
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    let mut received = b.received();

    received.extend(b.cycle());
    let mut expected = data::<S>([1, 2]);
    expected.push(Message::Flush("f".into()));
    assert_eq!(received, expected);
}

#[test]
fn left_spurious_wake_while_full_reads_nothing_past_the_held_message() {
    spurious_wake_while_full_reads_nothing_past_the_held_message::<Left>();
}

#[test]
fn right_spurious_wake_while_full_reads_nothing_past_the_held_message() {
    spurious_wake_while_full_reads_nothing_past_the_held_message::<Right>();
}

fn refused_messages_are_not_stranded<S: TestSide>() {
    let mut b = bounded::<S>(1);
    let mut cx = noop_context();
    for value in 0..5 {
        S::upstream(&b).push_back(Message::Data(value)).unwrap();
    }

    // The side runs only when woken, as on the poll thread: each wake must come from the routine.
    let _ = S::poll(&mut b).unwrap();
    let mut received = Vec::new();
    loop {
        let _ = b.output.poll(&mut cx).unwrap();
        received.extend(b.received());
        if !S::woken(&b).replace(false) {
            break;
        }
        let _ = S::poll(&mut b).unwrap();
    }
    assert_eq!(received, data::<S>(0..5));
}

#[test]
fn left_refused_messages_are_not_stranded() {
    refused_messages_are_not_stranded::<Left>();
}

#[test]
fn right_refused_messages_are_not_stranded() {
    refused_messages_are_not_stranded::<Right>();
}

fn takes_at_most_budget_then_wakes_only_itself<S: TestSide>() {
    let mut b = Biunion::new();
    let mut cx = noop_context();
    for value in 0..BUDGET + 8 {
        S::upstream(&b).push_back(Message::Data(value)).unwrap();
    }

    // One poll takes exactly BUDGET and owes itself, not the other side, the rest.
    let _ = S::poll(&mut b).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.received(), data::<S>(0..BUDGET));
    assert!(
        S::woken(&b).replace(false),
        "a budget stop must wake itself"
    );
    assert!(!S::Other::woken(&b).get());

    // The rest fits: an empty edge is upstream's to wake, not ours.
    let _ = S::poll(&mut b).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert_eq!(b.received(), data::<S>(BUDGET..BUDGET + 8));
    assert!(!S::woken(&b).get());
}

#[test]
fn left_takes_at_most_budget_then_wakes_only_itself() {
    takes_at_most_budget_then_wakes_only_itself::<Left>();
}

#[test]
fn right_takes_at_most_budget_then_wakes_only_itself() {
    takes_at_most_budget_then_wakes_only_itself::<Right>();
}

fn close_delivers_what_the_routine_still_accepts<S: TestSide>() {
    let mut b = bounded::<S>(2);
    let mut cx = noop_context();
    for value in 1..=3 {
        S::upstream(&b).push_back(Message::Data(value)).unwrap();
    }
    // 1 and 2 fit, 3 is held. Then upstream closes.
    let _ = S::poll(&mut b).unwrap();
    S::upstream(&b).close().unwrap();

    // Output makes room, so the held message still goes in before the side sees the close.
    let _ = b.output.poll(&mut cx).unwrap();
    assert!(S::woken(&b).get());
    assert!(matches!(
        S::poll(&mut b).unwrap_err().kind,
        ErrorKind::Closed
    ));

    let result = b.output.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert_eq!(b.received(), data::<S>(1..=3));
}

#[test]
fn left_close_delivers_what_the_routine_still_accepts() {
    close_delivers_what_the_routine_still_accepts::<Left>();
}

#[test]
fn right_close_delivers_what_the_routine_still_accepts() {
    close_delivers_what_the_routine_still_accepts::<Right>();
}

/// Once the routine makes room the side hands the held message over, sees the close, and
/// closes the node: nothing queued before the close is dropped.
fn hands_over_then_closes<S: TestSide>(b: &mut Biunion<Bounded>) {
    let mut cx = noop_context();
    b.woken.reset();
    let _ = b.output.poll(&mut cx).unwrap();
    assert!(S::woken(b).get());
    assert!(matches!(S::poll(b).unwrap_err().kind, ErrorKind::Closed));
    assert!(S::Other::woken(b).get());
    let result = b.output.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert_eq!(b.received(), data::<S>(1..=2));
}

/// A close behind a held message waits for the routine's room, which the refusal owes a wake.
fn close_while_holding_waits_for_room<S: TestSide>() {
    let mut b = bounded::<S>(1);
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();

    // 2 is held. The close wakes the side, which keeps holding.
    assert!(S::poll(&mut b).unwrap().is_pending());
    S::upstream(&b).close().unwrap();
    assert!(S::poll(&mut b).unwrap().is_pending());
    hands_over_then_closes::<S>(&mut b);
}

#[test]
fn left_close_while_holding_waits_for_room() {
    close_while_holding_waits_for_room::<Left>();
}

#[test]
fn right_close_while_holding_waits_for_room() {
    close_while_holding_waits_for_room::<Right>();
}

/// The close's wake can be spent on the poll that hits the refusal; the routine's wake still
/// comes once it makes room.
fn close_before_the_refusal_waits_for_room<S: TestSide>() {
    let mut b = bounded::<S>(1);
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    S::upstream(&b).close().unwrap();

    assert!(S::poll(&mut b).unwrap().is_pending());
    hands_over_then_closes::<S>(&mut b);
}

#[test]
fn left_close_before_the_refusal_waits_for_room() {
    close_before_the_refusal_waits_for_room::<Left>();
}

#[test]
fn right_close_before_the_refusal_waits_for_room() {
    close_before_the_refusal_waits_for_room::<Right>();
}

fn routine_returning_closed_from_try_send_becomes_fatal<S: TestSide>() {
    let mut b = Biunion::with(|_| ClosedTrySend);
    S::upstream(&b).push_back(Message::Data(1)).unwrap();

    let err = S::poll(&mut b).unwrap_err();
    assert!(
        matches!(err.kind, ErrorKind::Fatal(_)),
        "expected Fatal, got {:?}",
        err.kind
    );
}

#[test]
fn left_routine_returning_closed_from_try_send_becomes_fatal() {
    routine_returning_closed_from_try_send_becomes_fatal::<Left>();
}

#[test]
fn right_routine_returning_closed_from_try_send_becomes_fatal() {
    routine_returning_closed_from_try_send_becomes_fatal::<Right>();
}

// --- Backpressure, across sides: `S` holds, `S::Other` acts ---

fn one_side_holding_never_stops_the_other<S: TestSide>() {
    let mut b = bounded::<S>(1);
    let mut cx = noop_context();
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    let _ = S::poll(&mut b).unwrap();

    // S holds 2; the other side still flows.
    S::Other::upstream(&b).push_back(Message::Data(5)).unwrap();
    let _ = S::Other::poll(&mut b).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    let mut expected = data::<S>([1]);
    expected.extend(data::<S::Other>([5]));
    assert_eq!(b.received(), expected);
}

#[test]
fn left_holding_never_stops_right() {
    one_side_holding_never_stops_the_other::<Left>();
}

#[test]
fn right_holding_never_stops_left() {
    one_side_holding_never_stops_the_other::<Right>();
}

fn held_message_lands_after_the_other_sides_flush<S: TestSide>() {
    let mut b = bounded::<S>(1);
    let mut cx = noop_context();
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    let _ = S::poll(&mut b).unwrap();

    // The other side flushes while S holds 2.
    S::Other::upstream(&b)
        .push_back(Message::Flush("f".into()))
        .unwrap();
    let _ = S::Other::poll(&mut b).unwrap();

    // Output takes 1 out mid-flush: the routine wakes S, but S reads nothing until Running, so
    // that wake is spent.
    let _ = b.output.poll(&mut cx).unwrap();
    assert!(S::woken(&b).replace(false));
    let _ = S::poll(&mut b).unwrap();

    // The flush completes; Output's return to Running wakes S again, and 2 goes in after it.
    let _ = b.work.poll(&mut cx).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    assert!(S::woken(&b).replace(false));
    let _ = S::poll(&mut b).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    let mut expected = data::<S>([1]);
    expected.push(Message::Flush("f".into()));
    expected.extend(data::<S>([2]));
    assert_eq!(b.received(), expected);
}

#[test]
fn left_held_message_lands_after_rights_flush() {
    held_message_lands_after_the_other_sides_flush::<Left>();
}

#[test]
fn right_held_message_lands_after_lefts_flush() {
    held_message_lands_after_the_other_sides_flush::<Right>();
}

fn held_message_lands_after_the_other_sides_marker<S: TestSide>() {
    let mut b = bounded_mute::<S>(1);
    let mut cx = noop_context();
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    let _ = S::poll(&mut b).unwrap();

    S::Other::upstream(&b)
        .push_back(Message::Marker("m".into()))
        .unwrap();
    let _ = S::Other::poll(&mut b).unwrap();

    // Output drains 1 and forwards the Marker; the routine can't wake, so Output's return to
    // Running must wake both sides. S's 2 goes in after the Marker.
    b.woken.reset();
    let _ = b.output.poll(&mut cx).unwrap();
    assert!(S::woken(&b).replace(false));
    assert!(S::Other::woken(&b).get());
    let _ = S::poll(&mut b).unwrap();
    let _ = b.output.poll(&mut cx).unwrap();
    let mut expected = data::<S>([1]);
    expected.push(Message::Marker("m".into()));
    expected.extend(data::<S>([2]));
    assert_eq!(b.received(), expected);
}

#[test]
fn left_held_message_lands_after_rights_marker() {
    held_message_lands_after_the_other_sides_marker::<Left>();
}

#[test]
fn right_held_message_lands_after_lefts_marker() {
    held_message_lands_after_the_other_sides_marker::<Right>();
}

fn the_other_sides_close_tears_down_a_holding_side<S: TestSide>() {
    let mut b = bounded::<S>(1);
    let mut cx = noop_context();
    S::upstream(&b).push_back(Message::Data(1)).unwrap();
    S::upstream(&b).push_back(Message::Data(2)).unwrap();
    let _ = S::poll(&mut b).unwrap();

    // The other side closes while S holds 2: the node closes, and S must hear it.
    S::Other::upstream(&b).close().unwrap();
    b.woken.reset();
    let result = S::Other::poll(&mut b);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert!(S::woken(&b).get());

    // Every phase ends; the held 2 drops with the node.
    assert!(matches!(
        S::poll(&mut b).unwrap_err().kind,
        ErrorKind::Closed
    ));
    let result = b.work.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    let result = b.output.poll(&mut cx);
    assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));
    assert_eq!(b.received(), data::<S>([1]));
}

#[test]
fn rights_close_tears_down_a_holding_left() {
    the_other_sides_close_tears_down_a_holding_side::<Left>();
}

#[test]
fn lefts_close_tears_down_a_holding_right() {
    the_other_sides_close_tears_down_a_holding_side::<Right>();
}

#[test]
fn a_shared_budget_retries_both_held_sides() {
    let mut b = Biunion::with(|wakers| Bounded::new(Room::Shared(1), wakers));
    let mut cx = noop_context();
    b.upstream.left.push_back(Message::Data(1)).unwrap();
    b.upstream.left.push_back(Message::Data(2)).unwrap();
    b.upstream.right.push_back(Message::Data(5)).unwrap();

    // One slot: left takes it and holds 2; right holds 5.
    let _ = b.input.left.poll(&mut cx).unwrap();
    let _ = b.input.right.poll(&mut cx).unwrap();

    // Each pop frees the one slot and wakes both; they take turns until both are through.
    let mut received = Vec::new();
    loop {
        let _ = b.output.poll(&mut cx).unwrap();
        received.extend(b.received());
        let left = b.woken.input.left.replace(false);
        let right = b.woken.input.right.replace(false);
        if !left && !right {
            break;
        }
        if left {
            let _ = b.input.left.poll(&mut cx).unwrap();
        }
        if right {
            let _ = b.input.right.poll(&mut cx).unwrap();
        }
    }
    assert_eq!(
        received,
        [Message::Data(2), Message::Data(4), Message::Data(15)]
    );
}
