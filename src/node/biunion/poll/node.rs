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
use crate::{Closeable, Pollable, Receivable, ThreadId, fatal};
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
    Closing,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
                        self.work.target.flush()?;
                        self.state = NodeState::Closing;
                        // Work must poll routine until Ready to complete the close.
                        self.work.waker.wake();
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
                        self.work.target.flush()?;
                        self.state = NodeState::Closing;
                        self.work.waker.wake();
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::Closing | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_left()?;
                if matches!(s.state, NodeState::Closing) {
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::Closing | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_right()?;
                if matches!(s.state, NodeState::Closing) {
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
            NodeState::Closing => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    s.state = NodeState::CloseReady;
                    s.output.waker.wake();
                    return Err(crate::closed!());
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
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
    use crate::connect::poll::edge::SyncBridge;
    use crate::node::biunion::poll::routine::tests::{MockBiunion, noop_local_waker, noop_waker};
    use crate::{DefaultThread, SyncEdge};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestWaker(Arc<AtomicBool>);
    impl std::task::Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn test_waker() -> (std::task::Waker, Arc<AtomicBool>) {
        let woken = Arc::new(AtomicBool::new(false));
        let waker = std::task::Waker::from(Arc::new(TestWaker(woken.clone())));
        (waker, woken)
    }

    #[test]
    fn left_input_processes_data() {
        let (waker, _) = test_waker();
        let left_input = Arc::new(SyncBridge::new(waker));
        let (right_waker, _) = test_waker();
        let right_input = Arc::new(SyncBridge::new(right_waker));
        let output = Arc::new(SyncEdge::new());

        let phases = new_phases::<usize, usize, usize, &str, DefaultThread, _, _, _, _>(
            InputPhases {
                left: Phase {
                    target: left_input.clone(),
                    waker: noop_local_waker(),
                },
                right: Phase {
                    target: right_input.clone(),
                    waker: noop_local_waker(),
                },
            },
            Phase {
                target: MockBiunion::new(noop_local_waker()),
                waker: noop_local_waker(),
            },
            Phase {
                target: output.clone(),
                waker: noop_local_waker(),
            },
        );

        let mut left = phases.inputs.left;
        let mut work = phases.work;
        let mut out = phases.output;
        let mut wkr = noop_waker();

        left_input.push_back(Message::Data(5)).unwrap();
        let _ = left.poll(&mut wkr).unwrap();
        let _ = work.poll(&mut wkr).unwrap();
        let _ = out.poll(&mut wkr).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(10))); // 5 * 2
    }

    #[test]
    fn right_input_processes_data() {
        let (waker, _) = test_waker();
        let left_input = Arc::new(SyncBridge::new(waker));
        let (right_waker, _) = test_waker();
        let right_input = Arc::new(SyncBridge::new(right_waker));
        let output = Arc::new(SyncEdge::new());

        let phases = new_phases::<usize, usize, usize, &str, DefaultThread, _, _, _, _>(
            InputPhases {
                left: Phase {
                    target: left_input.clone(),
                    waker: noop_local_waker(),
                },
                right: Phase {
                    target: right_input.clone(),
                    waker: noop_local_waker(),
                },
            },
            Phase {
                target: MockBiunion::new(noop_local_waker()),
                waker: noop_local_waker(),
            },
            Phase {
                target: output.clone(),
                waker: noop_local_waker(),
            },
        );

        let mut right = phases.inputs.right;
        let mut work = phases.work;
        let mut out = phases.output;
        let mut wkr = noop_waker();

        right_input.push_back(Message::Data(5)).unwrap();
        let _ = right.poll(&mut wkr).unwrap();
        let _ = work.poll(&mut wkr).unwrap();
        let _ = out.poll(&mut wkr).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(15))); // 5 * 3
    }

    #[test]
    fn both_inputs_process_data() {
        let (waker, _) = test_waker();
        let left_input = Arc::new(SyncBridge::new(waker));
        let (right_waker, _) = test_waker();
        let right_input = Arc::new(SyncBridge::new(right_waker));
        let output = Arc::new(SyncEdge::new());

        let phases = new_phases::<usize, usize, usize, &str, DefaultThread, _, _, _, _>(
            InputPhases {
                left: Phase {
                    target: left_input.clone(),
                    waker: noop_local_waker(),
                },
                right: Phase {
                    target: right_input.clone(),
                    waker: noop_local_waker(),
                },
            },
            Phase {
                target: MockBiunion::new(noop_local_waker()),
                waker: noop_local_waker(),
            },
            Phase {
                target: output.clone(),
                waker: noop_local_waker(),
            },
        );

        let mut left = phases.inputs.left;
        let mut right = phases.inputs.right;
        let mut work = phases.work;
        let mut out = phases.output;
        let mut wkr = noop_waker();

        left_input.push_back(Message::Data(2)).unwrap();
        right_input.push_back(Message::Data(3)).unwrap();

        let _ = left.poll(&mut wkr).unwrap();
        let _ = right.poll(&mut wkr).unwrap();
        let _ = work.poll(&mut wkr).unwrap();
        let _ = out.poll(&mut wkr).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(4))); // 2 * 2
        assert_eq!(output.poll().unwrap(), Some(Message::Data(9))); // 3 * 3
    }

    #[test]
    fn flush_from_left_closes_via_work() {
        let (waker, _) = test_waker();
        let left_input = Arc::new(SyncBridge::new(waker));
        let (right_waker, _) = test_waker();
        let right_input = Arc::new(SyncBridge::<usize, &str>::new(right_waker));
        let output = Arc::new(SyncEdge::new());

        let phases = new_phases::<usize, usize, usize, &str, DefaultThread, _, _, _, _>(
            InputPhases {
                left: Phase {
                    target: left_input.clone(),
                    waker: noop_local_waker(),
                },
                right: Phase {
                    target: right_input.clone(),
                    waker: noop_local_waker(),
                },
            },
            Phase {
                target: MockBiunion::new(noop_local_waker()),
                waker: noop_local_waker(),
            },
            Phase {
                target: output.clone(),
                waker: noop_local_waker(),
            },
        );

        let mut left = phases.inputs.left;
        let mut work = phases.work;
        let mut out = phases.output;
        let mut wkr = noop_waker();

        left_input.push_back(Message::Flush("s1")).unwrap();
        let _ = left.poll(&mut wkr).unwrap();
        let _ = work.poll(&mut wkr).unwrap();
        let _ = out.poll(&mut wkr).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Flush("s1")));
    }

    #[test]
    fn close_from_left_propagates() {
        let (waker, _) = test_waker();
        let left_input = Arc::new(SyncBridge::<usize, &str>::new(waker));
        let (right_waker, _) = test_waker();
        let right_input = Arc::new(SyncBridge::<usize, &str>::new(right_waker));
        let output = Arc::new(SyncEdge::new());

        let phases = new_phases::<usize, usize, usize, &str, DefaultThread, _, _, _, _>(
            InputPhases {
                left: Phase {
                    target: left_input.clone(),
                    waker: noop_local_waker(),
                },
                right: Phase {
                    target: right_input.clone(),
                    waker: noop_local_waker(),
                },
            },
            Phase {
                target: MockBiunion::new(noop_local_waker()),
                waker: noop_local_waker(),
            },
            Phase {
                target: output.clone(),
                waker: noop_local_waker(),
            },
        );

        let mut left = phases.inputs.left;
        let mut work = phases.work;
        let mut out = phases.output;
        let mut wkr = noop_waker();

        SyncBridge::close(&left_input).unwrap();

        // Left detects close → Closing
        let _ = left.poll(&mut wkr);
        // Work polls routine → Ready → CloseReady
        let _ = work.poll(&mut wkr);
        // Output drains and closes
        let result = out.poll(&mut wkr);
        assert!(matches!(
            result.unwrap_err().kind,
            crate::error::ErrorKind::Closed
        ));
    }
}
