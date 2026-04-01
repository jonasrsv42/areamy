//! Async line node split into three phase pollables sharing state via
//! `Rc<RefCell<SharedState>>`:
//!
//! - **Input**: drain edge → Send to routine → wake Output
//! - **Work**: poll routine async work (woken by queue/IO)
//! - **Output**: drain Next → push downstream, forward flush/close signals
//!
//! State machine:
//! - **Running**: Input drains, Work polls, Output drains
//! - **Flushing**: Work polls routine until Ready → FlushReady
//! - **FlushReady**: Output drains remaining output, forwards Flush → Running
//! - **Closing**: Work polls routine until Ready → CloseReady
//! - **CloseReady**: Output drains remaining output, closes → Closed
//! - **Closed**: all phases return Closed

use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::{Closeable, Pollable, Receivable, ThreadId, fatal};
use std::cell::RefCell;
use std::rc::Rc;

/// Node state machine for managing flush and close lifecycle.
enum NodeState<SignalType> {
    Running,
    Flushing(SignalType),
    FlushReady(SignalType),
    Closing,
    CloseReady,
    Closed,
}

/// Shared state between Input, Work, and Output phases.
pub(crate) struct SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    pub(crate) worker: RoutineType,
    input: InputType,
    output: OutputType,
    state: NodeState<SignalType>,
    work_waker: std::task::Waker,
    output_waker: std::task::Waker,
}

impl<In, Out, SignalType, RoutineType, InputType, OutputType>
    SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    fn push_output(&mut self, msg: Message<Out, SignalType>) -> Result<(), Error> {
        self.output.push(msg)
    }

    /// Drain routine output via [crate::Next] and push downstream.
    fn drain_output(&mut self) -> Result<(), Error> {
        while let Some(out) = self.worker.next()? {
            self.push_output(Message::Data(out))?;
        }
        Ok(())
    }

    /// Drain input edge. Forwards data via [crate::Send], signals via
    /// [crate::Flush]. May transition state to Flushing or Closing.
    fn drain_input(&mut self) -> Result<(), Error> {
        loop {
            match self.input.try_recv() {
                Ok(Some(Message::Data(data))) => {
                    self.worker.send(data)?;
                }
                Ok(Some(Message::Flush(signal))) => {
                    self.worker.flush()?;
                    self.state = NodeState::Flushing(signal);
                    self.work_waker.wake_by_ref();
                    break;
                }
                Ok(Some(Message::Marker(signal))) => {
                    self.push_output(Message::Marker(signal))?;
                }
                Ok(None) => break,
                Err(e) => {
                    if matches!(e.kind, crate::error::ErrorKind::Closed) {
                        self.worker.flush()?;
                        self.state = NodeState::Closing;
                        self.work_waker.wake_by_ref();
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Poll routine. Closed error from routine → fatal.
    fn poll_routine(
        &mut self,
        cx: &mut core::task::Context<'_>,
    ) -> Result<core::task::Poll<()>, Error> {
        match crate::Poll::poll(&mut self.worker, cx) {
            Ok(poll) => Ok(poll),
            Err(e) if matches!(e.kind, crate::error::ErrorKind::Closed) => Err(fatal!(
                "routine returned Closed error — routines must handle close, not propagate it"
            )),
            Err(e) => Err(e),
        }
    }
}

// ============================================================
// Input phase
// ============================================================

/// Input phase: drains edge into routine, wakes Output.
pub struct Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    pub(crate) shared:
        Rc<RefCell<SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>>>,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Connection
    for Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::Closing | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_input()?;
                s.output_waker.wake_by_ref();
                if matches!(s.state, NodeState::Closing) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            // Flushing/FlushReady — Input waits for Output to transition back to Running
            NodeState::Flushing(_) | NodeState::FlushReady(_) => Ok(core::task::Poll::Pending),
        }
    }
}

// ============================================================
// Work phase
// ============================================================

/// Work phase: polls routine async work, transitions flush/close states.
pub struct Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    pub(crate) shared:
        Rc<RefCell<SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>>>,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Connection
    for Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Flushing(_) => {
                let result = s.poll_routine(cx)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                        NodeState::Flushing(sig) => sig,
                        _ => return Err(fatal!("expected Flushing state")),
                    };
                    s.state = NodeState::FlushReady(signal);
                    s.output_waker.wake_by_ref();
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::Closing => {
                let result = s.poll_routine(cx)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    s.state = NodeState::CloseReady;
                    s.output_waker.wake_by_ref();
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::Running => {
                let result = s.poll_routine(cx)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                Ok(core::task::Poll::Pending)
            }
            // FlushReady — Work is done, Output handles it
            NodeState::FlushReady(_) => Ok(core::task::Poll::Pending),
        }
    }
}

// ============================================================
// Output phase
// ============================================================

/// Output phase: drains routine output, forwards flush/close signals.
pub struct Output<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    pub(crate) shared:
        Rc<RefCell<SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>>>,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Connection
    for Output<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Output<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
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
                Ok(core::task::Poll::Pending)
            }
            NodeState::CloseReady => {
                s.drain_output()?;
                s.output.close()?;
                s.state = NodeState::Closed;
                Err(crate::closed!())
            }
            // Running, Flushing, Closing — drain any available output
            _ => {
                s.drain_output()?;
                Ok(core::task::Poll::Pending)
            }
        }
    }
}

/// Create Input, Work, and Output phase pollables sharing state.
pub fn new_phases<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>(
    worker: RoutineType,
    input: InputType,
    output: OutputType,
    work_waker: std::task::Waker,
    output_waker: std::task::Waker,
) -> (
    Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>,
    Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>,
    Output<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>,
)
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    let shared = Rc::new(RefCell::new(SharedState {
        worker,
        input,
        output,
        state: NodeState::Running,
        work_waker,
        output_waker,
    }));
    (
        Input {
            shared: shared.clone(),
            _thread_id: std::marker::PhantomData,
        },
        Work {
            shared: shared.clone(),
            _thread_id: std::marker::PhantomData,
        },
        Output {
            shared,
            _thread_id: std::marker::PhantomData,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::poll::sync_bridge::SyncBridge;
    use crate::node::line::routine::tests::AsyncMockLine;
    use crate::{DefaultThread, SyncEdge};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Waker;

    struct TestWaker(Arc<AtomicBool>);

    impl std::task::Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn test_waker() -> (Waker, Arc<AtomicBool>) {
        let woken = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(TestWaker(woken.clone())));
        (waker, woken)
    }

    #[test]
    fn poll_processes_input_and_produces_output() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::new(waker));
        let output = Arc::new(SyncEdge::new());

        let (mut input_phase, mut work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                AsyncMockLine::new(),
                input.clone(),
                output.clone(),
                std::task::Waker::noop().clone(),
                std::task::Waker::noop().clone(),
            );

        input.push_back(Message::Data(2)).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        // Input drains edge → Send to routine
        assert!(matches!(
            input_phase.poll(&mut cx).unwrap(),
            core::task::Poll::Pending
        ));

        // Work polls routine (sync mock — no-op here)
        assert!(matches!(
            work_phase.poll(&mut cx).unwrap(),
            core::task::Poll::Pending
        ));

        // Output drains Next → pushes downstream
        assert!(matches!(
            output_phase.poll(&mut cx).unwrap(),
            core::task::Poll::Pending
        ));

        assert_eq!(output.poll().unwrap(), Some(Message::Data(4)));
    }

    #[test]
    fn poll_returns_closed_on_closed_input() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::<usize, &str>::new(waker));
        let output = Arc::new(SyncEdge::new());

        let (mut input_phase, mut work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                AsyncMockLine::new(),
                input.clone(),
                output,
                std::task::Waker::noop().clone(),
                std::task::Waker::noop().clone(),
            );

        SyncBridge::close(&input).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        // Input detects closed → Closing
        let _ = input_phase.poll(&mut cx);

        // Work polls routine → Ready → CloseReady
        let _ = work_phase.poll(&mut cx);

        // Output drains, closes output → Closed
        let result = output_phase.poll(&mut cx);
        assert!(matches!(
            result.unwrap_err().kind,
            crate::error::ErrorKind::Closed
        ));
    }

    #[test]
    fn poll_calls_routine_poll() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::<usize, &str>::new(waker));
        let output = Arc::new(SyncEdge::new());

        let (_input_phase, mut work_phase, _output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                AsyncMockLine::new(),
                input,
                output,
                std::task::Waker::noop().clone(),
                std::task::Waker::noop().clone(),
            );

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = work_phase.poll(&mut cx).unwrap();
        assert_eq!(work_phase.shared.borrow().worker.poll_count, 1);

        let _ = work_phase.poll(&mut cx).unwrap();
        assert_eq!(work_phase.shared.borrow().worker.poll_count, 2);
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
        fn poll(
            &mut self,
            _cx: &mut core::task::Context<'_>,
        ) -> Result<core::task::Poll<()>, Error> {
            Err(crate::closed!())
        }
    }

    impl crate::node::Name for ClosedErrorRoutine {}
    impl crate::AsyncLineRoutine<usize, usize> for ClosedErrorRoutine {}

    #[test]
    fn routine_returning_closed_becomes_fatal() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::<usize, &str>::new(waker));
        let output = Arc::new(SyncEdge::new());

        let (_input_phase, mut work_phase, _output_phase) =
            new_phases::<_, _, _, DefaultThread, _, _, _>(
                ClosedErrorRoutine,
                input,
                output,
                std::task::Waker::noop().clone(),
                std::task::Waker::noop().clone(),
            );

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let err = work_phase.poll(&mut cx).unwrap_err();
        assert!(
            matches!(err.kind, crate::error::ErrorKind::Fatal(_)),
            "expected Fatal, got {:?}",
            err.kind
        );
    }

    #[test]
    fn routine_returning_closed_during_flush_becomes_fatal() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::new(waker));
        let output = Arc::new(SyncEdge::new());

        let (mut input_phase, mut work_phase, _output_phase) =
            new_phases::<_, _, _, DefaultThread, _, _, _>(
                ClosedErrorRoutine,
                input.clone(),
                output,
                std::task::Waker::noop().clone(),
                std::task::Waker::noop().clone(),
            );

        input.push_back(Message::Flush("s1")).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = input_phase.poll(&mut cx);

        let err = work_phase.poll(&mut cx).unwrap_err();
        assert!(
            matches!(err.kind, crate::error::ErrorKind::Fatal(_)),
            "expected Fatal, got {:?}",
            err.kind
        );
    }

    #[test]
    fn poll_drains_multiple_inputs() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::new(waker));
        let output = Arc::new(SyncEdge::new());

        let (mut input_phase, mut work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                AsyncMockLine::new(),
                input.clone(),
                output.clone(),
                std::task::Waker::noop().clone(),
                std::task::Waker::noop().clone(),
            );

        input.push_back(Message::Data(1)).unwrap();
        input.push_back(Message::Data(2)).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = input_phase.poll(&mut cx).unwrap();
        let _ = work_phase.poll(&mut cx).unwrap();
        let _ = output_phase.poll(&mut cx).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(2)));
        assert_eq!(output.poll().unwrap(), Some(Message::Data(6)));
    }
}
