//! Async biunion node split into four phase pollables sharing state via
//! `Rc<RefCell<SharedState>>`:
//!
//! - **LeftInput**: drain left edge → Send<Left, biunion::Left> to routine
//! - **RightInput**: drain right edge → Send<Right, biunion::Right> to routine
//! - **Work**: poll routine async work (woken by InputQueue/IO)
//! - **Output**: drain Next → push downstream, forward flush/close/marker signals
//!
//! State machine (same as line node):
//! - **Running**: Inputs drain, Work polls, Output drains
//! - **Flushing**: Work polls routine until Ready → FlushReady
//! - **FlushReady**: Output drains remaining output, forwards Flush → Running
//! - **MarkerReady**: Work polls routine once (best-effort), Output forwards Marker → Running
//! - **Closing**: Work polls routine until Ready → CloseReady
//! - **CloseReady**: Output drains remaining output, closes → Closed
//! - **Closed**: all phases return Closed
//!
//! Flush from either input triggers Flushing immediately (first-flush-wins).

use crate::biunion;
use crate::connect::waker::{ThreadLocalWaker, Waker};
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;
use crate::{Pollable, Receivable, Sink, ThreadId, fatal};
use std::cell::RefCell;
use std::rc::Rc;

/// A phase pairs its target (edge, routine, etc.) with its waker.
pub struct Phase<T> {
    pub target: T,
    pub waker: ThreadLocalWaker,
}

/// Both input phases grouped together.
pub struct InputPhases<LeftInputType, RightInputType> {
    pub left: Phase<LeftInputType>,
    pub right: Phase<RightInputType>,
}

/// Node state machine for managing flush and close lifecycle.
enum NodeState<SignalType> {
    Running,
    Flushing(SignalType),
    FlushReady(SignalType),
    MarkerReady(SignalType),
    CloseReady,
    Closed,
}

/// Shared state between LeftInput, RightInput, Work, and Output phases.
pub(crate) struct SharedState<
    Left,
    Right,
    Out,
    SignalType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    input: InputPhases<LeftInputType, RightInputType>,
    work: Phase<RoutineType>,
    output: Phase<OutputType>,
    state: NodeState<SignalType>,
}

impl<Left, Right, Out, SignalType, RoutineType, LeftInputType, RightInputType, OutputType>
    SharedState<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    fn push_output(&mut self, msg: Message<Out, SignalType>) -> Result<(), Error> {
        self.output.target.push(msg)
    }

    fn drain_output(&mut self) -> Result<(), Error> {
        while let Some(out) = self.work.target.next()? {
            self.push_output(Message::Data(out))?;
        }
        Ok(())
    }

    fn drain_left(&mut self) -> Result<(), Error> {
        loop {
            match self.input.left.target.try_recv() {
                Ok(Some(Message::Data(data))) => {
                    crate::Send::<Left, biunion::Left>::send(&mut self.work.target, data)?;
                }
                Ok(Some(Message::Flush(signal))) => {
                    self.work.target.flush()?;
                    self.state = NodeState::Flushing(signal);
                    // Work must poll routine until Ready to complete the flush.
                    self.work.waker.wake();
                    break;
                }
                Ok(Some(Message::Marker(signal))) => {
                    self.state = NodeState::MarkerReady(signal);
                    // Work gets one poll cycle before Output forwards the marker.
                    self.work.waker.wake();
                    break;
                }
                Ok(None) => break,
                Err(e) => {
                    if matches!(e.kind, crate::error::ErrorKind::Closed) {
                        // Fast close: skip flush, skip routine polling.
                        // Output drains buffered next() and closes the
                        // downstream edge; routine drops with the node.
                        self.state = NodeState::CloseReady;
                        self.work.waker.wake();
                        self.output.waker.wake();
                        // Wake the other input so it sees CloseReady and
                        // returns Closed.
                        self.input.right.waker.wake();
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn drain_right(&mut self) -> Result<(), Error> {
        loop {
            match self.input.right.target.try_recv() {
                Ok(Some(Message::Data(data))) => {
                    crate::Send::<Right, biunion::Right>::send(&mut self.work.target, data)?;
                }
                Ok(Some(Message::Flush(signal))) => {
                    self.work.target.flush()?;
                    self.state = NodeState::Flushing(signal);
                    self.work.waker.wake();
                    break;
                }
                Ok(Some(Message::Marker(signal))) => {
                    self.state = NodeState::MarkerReady(signal);
                    self.work.waker.wake();
                    break;
                }
                Ok(None) => break,
                Err(e) => {
                    if matches!(e.kind, crate::error::ErrorKind::Closed) {
                        // Fast close — see `drain_left` for rationale.
                        self.state = NodeState::CloseReady;
                        self.work.waker.wake();
                        self.output.waker.wake();
                        // Wake the other input so it sees CloseReady and
                        // returns Closed.
                        self.input.left.waker.wake();
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn poll_routine(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        match crate::Poll::poll(&mut self.work.target, waker) {
            Ok(poll) => Ok(poll),
            Err(e) if matches!(e.kind, crate::error::ErrorKind::Closed) => Err(fatal!(
                "routine returned Closed error — routines must handle close, not propagate it"
            )),
            Err(e) => Err(e),
        }
    }
}

// ============================================================
// Shared type alias for the Rc<RefCell<SharedState<...>>>
// ============================================================

type Shared<Left, Right, Out, SignalType, RoutineType, LeftInputType, RightInputType, OutputType> =
    Rc<
        RefCell<
            SharedState<
                Left,
                Right,
                Out,
                SignalType,
                RoutineType,
                LeftInputType,
                RightInputType,
                OutputType,
            >,
        >,
    >;

// ============================================================
// LeftInput phase
// ============================================================

pub struct LeftInput<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    shared: Shared<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Connection
    for LeftInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Pollable
    for LeftInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_left()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            _ => Ok(core::task::Poll::Pending),
        }
    }
}

// ============================================================
// RightInput phase
// ============================================================

pub struct RightInput<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    shared: Shared<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Connection
    for RightInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Pollable
    for RightInput<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_right()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            _ => Ok(core::task::Poll::Pending),
        }
    }
}

// ============================================================
// Work phase
// ============================================================

pub struct Work<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    shared: Shared<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Connection
    for Work<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Pollable
    for Work<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Flushing(_) => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                        NodeState::Flushing(sig) => sig,
                        _ => return Err(fatal!("expected Flushing state")),
                    };
                    s.state = NodeState::FlushReady(signal);
                    s.output.waker.wake();
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::Running => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                s.output.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::FlushReady(_) => Ok(core::task::Poll::Pending),
        }
    }
}

// ============================================================
// Output phase
// ============================================================

pub struct Output<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    shared: Shared<
        Left,
        Right,
        Out,
        SignalType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Connection
    for Output<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
}

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> Pollable
    for Output<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed => Err(crate::closed!()),
            NodeState::FlushReady(_) => {
                s.drain_output()?;
                let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                    NodeState::FlushReady(sig) => sig,
                    _ => return Err(fatal!("expected FlushReady state")),
                };
                s.push_output(Message::Flush(signal))?;
                // Flush forwarded, back to Running. Both inputs can resume.
                s.input.left.waker.wake();
                s.input.right.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                s.drain_output()?;
                let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                    NodeState::MarkerReady(sig) => sig,
                    _ => return Err(fatal!("expected MarkerReady state")),
                };
                s.push_output(Message::Marker(signal))?;
                // Marker forwarded, back to Running. Both inputs can resume.
                s.input.left.waker.wake();
                s.input.right.waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::CloseReady => {
                s.drain_output()?;
                s.output.target.close()?;
                s.state = NodeState::Closed;
                Err(crate::closed!())
            }
            _ => {
                s.drain_output()?;
                Ok(core::task::Poll::Pending)
            }
        }
    }
}

/// Create all four phase pollables sharing state.
pub fn new_phases<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
>(
    input: InputPhases<LeftInputType, RightInputType>,
    work: Phase<RoutineType>,
    output: Phase<OutputType>,
) -> super::phases::Phases<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Sink<DataType = Out, SignalType = SignalType>,
{
    let shared = Rc::new(RefCell::new(SharedState {
        input,
        work,
        output,
        state: NodeState::Running,
    }));
    super::phases::Phases {
        inputs: super::phases::Inputs {
            left: LeftInput {
                shared: shared.clone(),
                _thread_id: std::marker::PhantomData,
            },
            right: RightInput {
                shared: shared.clone(),
                _thread_id: std::marker::PhantomData,
            },
        },
        work: Work {
            shared: shared.clone(),
            _thread_id: std::marker::PhantomData,
        },
        output: Output {
            shared,
            _thread_id: std::marker::PhantomData,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultThread;
    use crate::connect::poll::input;
    use crate::connect::sync::{Receiver, Sender};
    use crate::connect::waker::ThreadLocalWaker;
    use crate::node::biunion::poll::routine::tests::{
        MockBiunion, noop_biunion_wakers, noop_waker,
    };
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestWaker(Arc<AtomicBool>);
    impl std::task::Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn sync_waker() -> std::task::Waker {
        let flag = Arc::new(AtomicBool::new(false));
        std::task::Waker::from(Arc::new(TestWaker(flag)))
    }

    struct TrackWake(Rc<Cell<bool>>);
    impl crate::connect::waker::ThreadLocalWake for TrackWake {
        fn wake(&self) {
            self.0.set(true);
        }
        fn schedule_at(&self, _: std::time::Instant) {}
    }

    fn track_local_waker() -> (ThreadLocalWaker, Rc<Cell<bool>>) {
        let woken = Rc::new(Cell::new(false));
        (ThreadLocalWaker::new(TrackWake(woken.clone())), woken)
    }

    type TestLeftInput = LeftInput<
        usize,
        usize,
        usize,
        &'static str,
        DefaultThread,
        MockBiunion,
        input::sync::Receiver<usize, &'static str>,
        input::sync::Receiver<usize, &'static str>,
        Sender<usize, &'static str>,
    >;
    type TestRightInput = RightInput<
        usize,
        usize,
        usize,
        &'static str,
        DefaultThread,
        MockBiunion,
        input::sync::Receiver<usize, &'static str>,
        input::sync::Receiver<usize, &'static str>,
        Sender<usize, &'static str>,
    >;
    type TestWork = Work<
        usize,
        usize,
        usize,
        &'static str,
        DefaultThread,
        MockBiunion,
        input::sync::Receiver<usize, &'static str>,
        input::sync::Receiver<usize, &'static str>,
        Sender<usize, &'static str>,
    >;
    type TestOutput = Output<
        usize,
        usize,
        usize,
        &'static str,
        DefaultThread,
        MockBiunion,
        input::sync::Receiver<usize, &'static str>,
        input::sync::Receiver<usize, &'static str>,
        Sender<usize, &'static str>,
    >;

    struct Harness {
        left_edge: input::sync::Sender<usize, &'static str>,
        right_edge: input::sync::Sender<usize, &'static str>,
        output_edge: Receiver<usize, &'static str>,
        left: TestLeftInput,
        right: TestRightInput,
        work: TestWork,
        output: TestOutput,
        left_woken: Rc<Cell<bool>>,
        right_woken: Rc<Cell<bool>>,
        work_woken: Rc<Cell<bool>>,
        output_woken: Rc<Cell<bool>>,
        waker: Waker,
    }

    impl Harness {
        fn new() -> Self {
            let left_recv = input::sync::Receiver::new(sync_waker());
            let left_edge = left_recv.sender();
            let right_recv = input::sync::Receiver::new(sync_waker());
            let right_edge = right_recv.sender();
            let output_edge = Receiver::new();
            let output_sender = output_edge.sender();

            let (left_local, left_woken) = track_local_waker();
            let (right_local, right_woken) = track_local_waker();
            let (work_local, work_woken) = track_local_waker();
            let (output_local, output_woken) = track_local_waker();

            let phases = new_phases::<usize, usize, usize, &str, DefaultThread, _, _, _, _>(
                InputPhases {
                    left: Phase {
                        target: left_recv,
                        waker: left_local,
                    },
                    right: Phase {
                        target: right_recv,
                        waker: right_local,
                    },
                },
                Phase {
                    target: MockBiunion::new(noop_biunion_wakers()),
                    waker: work_local,
                },
                Phase {
                    target: output_sender,
                    waker: output_local,
                },
            );

            Harness {
                left_edge,
                right_edge,
                output_edge,
                left: phases.inputs.left,
                right: phases.inputs.right,
                work: phases.work,
                output: phases.output,
                left_woken,
                right_woken,
                work_woken,
                output_woken,
                waker: noop_waker(),
            }
        }

        fn reset_woken(&self) {
            self.left_woken.set(false);
            self.right_woken.set(false);
            self.work_woken.set(false);
            self.output_woken.set(false);
        }

        fn poll_left(&mut self) -> Result<core::task::Poll<()>, Error> {
            self.left.poll(&mut self.waker)
        }

        fn poll_right(&mut self) -> Result<core::task::Poll<()>, Error> {
            self.right.poll(&mut self.waker)
        }

        fn poll_work(&mut self) -> Result<core::task::Poll<()>, Error> {
            self.work.poll(&mut self.waker)
        }

        fn poll_output(&mut self) -> Result<core::task::Poll<()>, Error> {
            self.output.poll(&mut self.waker)
        }

        fn read_output(&self) -> Option<Message<usize, &'static str>> {
            self.output_edge.poll().unwrap()
        }
    }

    // --- Data flow ---

    #[test]
    fn left_input_processes_data() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Data(5)).unwrap();
        let _ = h.poll_left().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Data(10))); // 5 * 2
    }

    #[test]
    fn right_input_processes_data() {
        let mut h = Harness::new();
        h.right_edge.push_back(Message::Data(5)).unwrap();
        let _ = h.poll_right().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Data(15))); // 5 * 3
    }

    #[test]
    fn both_inputs_process_data() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Data(2)).unwrap();
        h.right_edge.push_back(Message::Data(3)).unwrap();
        let _ = h.poll_left().unwrap();
        let _ = h.poll_right().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Data(4))); // 2 * 2
        assert_eq!(h.read_output(), Some(Message::Data(9))); // 3 * 3
    }

    // --- Signal forwarding ---

    #[test]
    fn flush_from_left_forwards() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Flush("s1")).unwrap();
        let _ = h.poll_left().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Flush("s1")));
    }

    #[test]
    fn flush_from_right_forwards() {
        let mut h = Harness::new();
        h.right_edge.push_back(Message::Flush("s1")).unwrap();
        let _ = h.poll_right().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Flush("s1")));
    }

    #[test]
    fn marker_from_left_forwards() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Marker("m1")).unwrap();
        let _ = h.poll_left().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Marker("m1")));
    }

    #[test]
    fn marker_from_right_forwards() {
        let mut h = Harness::new();
        h.right_edge.push_back(Message::Marker("m1")).unwrap();
        let _ = h.poll_right().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Marker("m1")));
    }

    // --- Close propagation ---

    #[test]
    fn close_from_left_propagates() {
        let mut h = Harness::new();
        h.left_edge.close().unwrap();
        let _ = h.poll_left();
        assert!(matches!(
            h.poll_right().unwrap_err().kind,
            crate::error::ErrorKind::Closed
        ));
        let _ = h.poll_work();
        assert!(matches!(
            h.poll_output().unwrap_err().kind,
            crate::error::ErrorKind::Closed
        ));
    }

    #[test]
    fn data_then_close_drains_before_closing() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Data(7)).unwrap();
        let _ = h.poll_left().unwrap();
        let _ = h.poll_work().unwrap();
        let _ = h.poll_output().unwrap();
        assert_eq!(h.read_output(), Some(Message::Data(14))); // 7 * 2

        h.left_edge.close().unwrap();
        let _ = h.poll_left();
        let _ = h.poll_work();
        assert!(matches!(
            h.poll_output().unwrap_err().kind,
            crate::error::ErrorKind::Closed
        ));
    }

    // --- Input waker: close wakes other input ---

    #[test]
    fn close_from_left_wakes_right() {
        let mut h = Harness::new();
        h.reset_woken();
        h.left_edge.close().unwrap();
        let _ = h.poll_left();
        assert!(h.right_woken.get(), "closing left must wake right input");
    }

    #[test]
    fn close_from_right_wakes_left() {
        let mut h = Harness::new();
        h.reset_woken();
        h.right_edge.close().unwrap();
        let _ = h.poll_right();
        assert!(h.left_woken.get(), "closing right must wake left input");
    }

    // --- Input waker: close/flush/marker wakes work ---

    #[test]
    fn close_from_left_wakes_work() {
        let mut h = Harness::new();
        h.reset_woken();
        h.left_edge.close().unwrap();
        let _ = h.poll_left();
        assert!(h.work_woken.get(), "closing left must wake work phase");
    }

    #[test]
    fn close_from_right_wakes_work() {
        let mut h = Harness::new();
        h.reset_woken();
        h.right_edge.close().unwrap();
        let _ = h.poll_right();
        assert!(h.work_woken.get(), "closing right must wake work phase");
    }

    #[test]
    fn flush_from_left_wakes_work() {
        let mut h = Harness::new();
        h.reset_woken();
        h.left_edge.push_back(Message::Flush("s1")).unwrap();
        let _ = h.poll_left().unwrap();
        assert!(h.work_woken.get(), "flush from left must wake work phase");
    }

    #[test]
    fn flush_from_right_wakes_work() {
        let mut h = Harness::new();
        h.reset_woken();
        h.right_edge.push_back(Message::Flush("s1")).unwrap();
        let _ = h.poll_right().unwrap();
        assert!(h.work_woken.get(), "flush from right must wake work phase");
    }

    #[test]
    fn marker_from_left_wakes_work() {
        let mut h = Harness::new();
        h.reset_woken();
        h.left_edge.push_back(Message::Marker("m1")).unwrap();
        let _ = h.poll_left().unwrap();
        assert!(h.work_woken.get(), "marker from left must wake work phase");
    }

    #[test]
    fn marker_from_right_wakes_work() {
        let mut h = Harness::new();
        h.reset_woken();
        h.right_edge.push_back(Message::Marker("m1")).unwrap();
        let _ = h.poll_right().unwrap();
        assert!(h.work_woken.get(), "marker from right must wake work phase");
    }

    // --- Work waker: flush/close/marker ready wakes output ---

    #[test]
    fn flush_ready_wakes_output() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Flush("s1")).unwrap();
        let _ = h.poll_left().unwrap();
        h.reset_woken();
        let _ = h.poll_work().unwrap();
        assert!(
            h.output_woken.get(),
            "work must wake output when flush ready"
        );
    }

    #[test]
    fn close_wakes_output_directly() {
        // Fast close: drain_left transitions straight to CloseReady and
        // wakes Output directly (no flush, no work polling).
        let mut h = Harness::new();
        h.left_edge.close().unwrap();
        h.reset_woken();
        let _ = h.poll_left();
        assert!(h.output_woken.get(), "drain_left must wake output on close");
    }

    #[test]
    fn marker_ready_wakes_output() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Marker("m1")).unwrap();
        let _ = h.poll_left().unwrap();
        h.reset_woken();
        let _ = h.poll_work().unwrap();
        assert!(
            h.output_woken.get(),
            "work must wake output when marker ready"
        );
    }

    // --- Output waker: flush/marker forwarded wakes both inputs ---

    #[test]
    fn flush_forwarded_wakes_both_inputs() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Flush("s1")).unwrap();
        let _ = h.poll_left().unwrap();
        let _ = h.poll_work().unwrap();
        h.reset_woken();
        let _ = h.poll_output().unwrap();
        assert!(
            h.left_woken.get(),
            "output must wake left after flush forwarded"
        );
        assert!(
            h.right_woken.get(),
            "output must wake right after flush forwarded"
        );
    }

    #[test]
    fn marker_forwarded_wakes_both_inputs() {
        let mut h = Harness::new();
        h.left_edge.push_back(Message::Marker("m1")).unwrap();
        let _ = h.poll_left().unwrap();
        let _ = h.poll_work().unwrap();
        h.reset_woken();
        let _ = h.poll_output().unwrap();
        assert!(
            h.left_woken.get(),
            "output must wake left after marker forwarded"
        );
        assert!(
            h.right_woken.get(),
            "output must wake right after marker forwarded"
        );
    }
}
