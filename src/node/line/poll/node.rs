//! Async line node split into three phase pollables sharing state via
//! `Rc<RefCell<SharedState>>`:
//!
//! - **Input**: drain edge → Send to routine. Routine wakes Output via OutputProducer.
//! - **Work**: poll routine async work (woken by InputQueue/IO)
//! - **Output**: drain Next → push downstream, forward flush/close/marker signals
//!
//! State machine:
//! - **Running**: Input drains, Work polls, Output drains
//! - **Flushing**: Work polls routine until Ready → FlushReady
//! - **FlushReady**: Output drains remaining output, forwards Flush → Running
//! - **MarkerReady**: Work polls routine once (best-effort drain), Output forwards Marker → Running
//! - **CloseReady**: Output drains buffered output, closes → Closed
//! - **Closed**: all phases return Closed
//!
//! # Close vs Flush
//!
//! Flush calls `routine.flush()` and polls until Ready — graceful
//! wrap-up. Close is a *fast* path: no flush, no poll. Output drains
//! whatever is already buffered in `routine.next()` and closes the
//! downstream edge; the routine drops with the node, so its `Drop`
//! is responsible for cancelling any in-flight async work.
//!
//! # Marker ordering
//!
//! Markers are synchronization primitives — all data naturally produced
//! before the marker should arrive downstream before it. For sync routines
//! this is guaranteed: Send produces output immediately, Output drains it,
//! then forwards the marker.
//!
//! For async routines (e.g. FutureRoutine), the guarantee is **best-effort**.
//! Work gets one poll cycle to let the routine emit pending output before
//! Output forwards the marker. Data produced by the routine *after* that
//! poll cycle (e.g. from an in-flight future) may arrive after the marker.
//! If strict ordering is required with async routines, use Flush instead.

use crate::connect::waker::{ThreadLocalWaker, Waker};
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::node::line::poll::routine::LineRoutine;
use crate::signal::Origin;
use crate::{Closeable, Pollable, Receivable, ThreadId, fatal};
use std::cell::RefCell;
use std::rc::Rc;

/// Node state machine for managing flush and close lifecycle.
enum NodeState<SignalType> {
    Running,
    Flushing(SignalType),
    FlushReady(SignalType),
    MarkerReady(SignalType),
    CloseReady,
    Closed,
}

/// Shared state between Input, Work, and Output phases.
pub(crate) struct SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    pub(crate) worker: RoutineType,
    input: InputType,
    output: OutputType,
    state: NodeState<SignalType>,
    input_waker: ThreadLocalWaker,
    work_waker: ThreadLocalWaker,
    output_waker: ThreadLocalWaker,
}

impl<In, Out, SignalType, RoutineType, InputType, OutputType>
    SharedState<In, Out, SignalType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    RoutineType: LineRoutine<In, Out>,
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

    /// Drain input edge. Forwards data via [crate::Send].
    /// May transition state to Flushing, Closing, or MarkerReady.
    fn drain_input(&mut self) -> Result<(), Error> {
        loop {
            match self.input.try_recv() {
                Ok(Some(Message::Data(data))) => {
                    self.worker.send(data)?;
                }
                Ok(Some(Message::Flush(signal))) => {
                    self.worker.flush()?;
                    self.state = NodeState::Flushing(signal);
                    // Work must poll routine until Ready to complete the flush.
                    self.work_waker.wake();
                    break;
                }
                Ok(Some(Message::Marker(signal))) => {
                    self.state = NodeState::MarkerReady(signal);
                    // Work gets one poll cycle to let routine emit pending output
                    // before Output forwards the marker.
                    self.work_waker.wake();
                    break;
                }
                Ok(None) => break,
                Err(e) => {
                    if matches!(e.kind, crate::error::ErrorKind::Closed) {
                        // Fast close: skip flush, skip routine polling.
                        // Output drains buffered next() and closes the
                        // downstream edge; routine is dropped with the node.
                        // Wake Work so it observes CloseReady and reports
                        // closed back to the poll loop; wake Output to
                        // drive the drain + close.
                        self.state = NodeState::CloseReady;
                        self.work_waker.wake();
                        self.output_waker.wake();
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Poll routine. Builds std Context from sync waker for the routine.
    /// Closed error from routine fatal.
    fn poll_routine(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        match crate::Poll::poll(&mut self.worker, waker) {
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
    RoutineType: LineRoutine<In, Out>,
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
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        let mut s = self.shared.borrow_mut();
        match &s.state {
            NodeState::Closed | NodeState::CloseReady => Err(crate::closed!()),
            NodeState::Running => {
                s.drain_input()?;
                if matches!(s.state, NodeState::CloseReady) {
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }
            // Flushing/FlushReady/MarkerReady — Input waits for Output to transition back to Running
            NodeState::Flushing(_) | NodeState::FlushReady(_) | NodeState::MarkerReady(_) => {
                Ok(core::task::Poll::Pending)
            }
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
    RoutineType: LineRoutine<In, Out>,
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
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    // Work polls the routine. The routine is responsible for waking
    // Output when it produces data (via OutputProducer). Work only
    // wakes Output on state transitions (FlushReady, CloseReady)
    // where Output needs to forward signals downstream.
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
                    // Flush complete. Output must drain remaining output
                    // and forward the Flush signal downstream.
                    s.output_waker.wake();
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
                // No wake — routine is responsible for waking Output
                // when it produces data (via OutputProducer).
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                let result = s.poll_routine(waker)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                // Routine got one poll cycle. Output must drain any
                // produced output and forward the marker downstream.
                s.output_waker.wake();
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
    RoutineType: LineRoutine<In, Out>,
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
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for Output<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
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
                // Flush forwarded, back to Running. Input can resume draining.
                s.input_waker.wake();
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                s.drain_output()?;
                let signal = match std::mem::replace(&mut s.state, NodeState::Running) {
                    NodeState::MarkerReady(sig) => sig,
                    _ => return Err(fatal!("expected MarkerReady state")),
                };
                s.push_output(Message::Marker(signal))?;
                // Marker forwarded, back to Running. Input can resume draining.
                s.input_waker.wake();
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
    input_waker: ThreadLocalWaker,
    work_waker: ThreadLocalWaker,
    output_waker: ThreadLocalWaker,
) -> (
    Input<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>,
    Work<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>,
    Output<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>,
)
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    let shared = Rc::new(RefCell::new(SharedState {
        worker,
        input,
        output,
        state: NodeState::Running,
        input_waker,
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
    use crate::DefaultThread;
    use crate::connect::poll::input;
    use crate::connect::sync::Receiver;
    use crate::connect::waker::{self, ThreadLocalWake, ThreadLocalWaker};
    use crate::node::line::poll::routine::tests::{MockLine, noop_line_wakers};
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

    struct NoopLocalWake;
    impl ThreadLocalWake for NoopLocalWake {
        fn wake(&self) {}
        fn schedule_at(&self, _: std::time::Instant) {}
    }

    fn test_waker() -> (std::task::Waker, Arc<AtomicBool>) {
        let woken = Arc::new(AtomicBool::new(false));
        let waker = std::task::Waker::from(Arc::new(TestWaker(woken.clone())));
        (waker, woken)
    }

    fn noop_local_waker() -> ThreadLocalWaker {
        ThreadLocalWaker::new(NoopLocalWake)
    }

    fn noop_waker() -> waker::Waker {
        waker::Waker {
            sync: std::task::Waker::noop().clone(),
            local: noop_local_waker(),
        }
    }

    #[test]
    fn poll_processes_input_and_produces_output() {
        let (waker, _) = test_waker();
        let input_recv = input::sync::Receiver::new(waker);
        let input = input_recv.sender();
        let output = Receiver::new();

        let (mut input_phase, mut work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                MockLine::new(noop_line_wakers()),
                input_recv,
                output.sender(),
                noop_local_waker(),
                noop_local_waker(),
                noop_local_waker(),
            );

        input.push_back(Message::Data(2)).unwrap();

        let mut wkr = noop_waker();

        // Input drains edge → Send to routine
        assert!(matches!(
            input_phase.poll(&mut wkr).unwrap(),
            core::task::Poll::Pending
        ));

        // Work polls routine (sync mock — no-op here)
        assert!(matches!(
            work_phase.poll(&mut wkr).unwrap(),
            core::task::Poll::Pending
        ));

        // Output drains Next → pushes downstream
        assert!(matches!(
            output_phase.poll(&mut wkr).unwrap(),
            core::task::Poll::Pending
        ));

        assert_eq!(output.poll().unwrap(), Some(Message::Data(4)));
    }

    #[test]
    fn poll_returns_closed_on_closed_input() {
        let (waker, _) = test_waker();
        let input_recv = input::sync::Receiver::<usize, &str>::new(waker);
        let input = input_recv.sender();
        let output = Receiver::new();

        let (mut input_phase, _work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                MockLine::new(noop_line_wakers()),
                input_recv,
                output.sender(),
                noop_local_waker(),
                noop_local_waker(),
                noop_local_waker(),
            );

        input.close().unwrap();

        let mut wkr = noop_waker();

        // Fast close: Input detects closed → CloseReady directly,
        // skipping the Closing/poll-routine cycle. Work is irrelevant.
        let result = input_phase.poll(&mut wkr);
        assert!(matches!(
            result.unwrap_err().kind,
            crate::error::ErrorKind::Closed
        ));

        // Output drains buffered output (none here) and closes downstream.
        let result = output_phase.poll(&mut wkr);
        assert!(matches!(
            result.unwrap_err().kind,
            crate::error::ErrorKind::Closed
        ));
    }

    #[test]
    fn poll_calls_routine_poll() {
        let (waker, _) = test_waker();
        let input_recv = input::sync::Receiver::<usize, &str>::new(waker);
        let output = Receiver::new();

        let (_input_phase, mut work_phase, _output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                MockLine::new(noop_line_wakers()),
                input_recv,
                output.sender(),
                noop_local_waker(),
                noop_local_waker(),
                noop_local_waker(),
            );

        let mut wkr = noop_waker();

        let _ = work_phase.poll(&mut wkr).unwrap();
        assert_eq!(work_phase.shared.borrow().worker.poll_count, 1);

        let _ = work_phase.poll(&mut wkr).unwrap();
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
        fn poll(&mut self, _waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
            Err(crate::closed!())
        }
    }

    impl crate::node::Name for ClosedErrorRoutine {}
    impl LineRoutine<usize, usize> for ClosedErrorRoutine {}

    #[test]
    fn routine_returning_closed_becomes_fatal() {
        let (waker, _) = test_waker();
        let input_recv = input::sync::Receiver::<usize, &str>::new(waker);
        let output = Receiver::new();

        let (_input_phase, mut work_phase, _output_phase) =
            new_phases::<_, _, _, DefaultThread, _, _, _>(
                ClosedErrorRoutine,
                input_recv,
                output.sender(),
                noop_local_waker(),
                noop_local_waker(),
                noop_local_waker(),
            );

        let mut wkr = noop_waker();

        let err = work_phase.poll(&mut wkr).unwrap_err();
        assert!(
            matches!(err.kind, crate::error::ErrorKind::Fatal(_)),
            "expected Fatal, got {:?}",
            err.kind
        );
    }

    #[test]
    fn routine_returning_closed_during_flush_becomes_fatal() {
        let (waker, _) = test_waker();
        let input_recv = input::sync::Receiver::new(waker);
        let input = input_recv.sender();
        let output = Receiver::new();

        let (mut input_phase, mut work_phase, _output_phase) =
            new_phases::<_, _, _, DefaultThread, _, _, _>(
                ClosedErrorRoutine,
                input_recv,
                output.sender(),
                noop_local_waker(),
                noop_local_waker(),
                noop_local_waker(),
            );

        input.push_back(Message::Flush("s1")).unwrap();

        let mut wkr = noop_waker();

        let _ = input_phase.poll(&mut wkr);

        let err = work_phase.poll(&mut wkr).unwrap_err();
        assert!(
            matches!(err.kind, crate::error::ErrorKind::Fatal(_)),
            "expected Fatal, got {:?}",
            err.kind
        );
    }

    #[test]
    fn poll_drains_multiple_inputs() {
        let (waker, _) = test_waker();
        let input_recv = input::sync::Receiver::new(waker);
        let input = input_recv.sender();
        let output = Receiver::new();

        let (mut input_phase, mut work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                MockLine::new(noop_line_wakers()),
                input_recv,
                output.sender(),
                noop_local_waker(),
                noop_local_waker(),
                noop_local_waker(),
            );

        input.push_back(Message::Data(1)).unwrap();
        input.push_back(Message::Data(2)).unwrap();

        let mut wkr = noop_waker();

        let _ = input_phase.poll(&mut wkr).unwrap();
        let _ = work_phase.poll(&mut wkr).unwrap();
        let _ = output_phase.poll(&mut wkr).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(2)));
        assert_eq!(output.poll().unwrap(), Some(Message::Data(6)));
    }

    #[test]
    fn marker_preserves_ordering_with_data() {
        let (waker, _) = test_waker();
        let input_recv = input::sync::Receiver::new(waker);
        let input = input_recv.sender();
        let output = Receiver::new();

        let (mut input_phase, mut work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                MockLine::new(noop_line_wakers()),
                input_recv,
                output.sender(),
                noop_local_waker(),
                noop_local_waker(),
                noop_local_waker(),
            );

        // Data then Marker then more Data
        input.push_back(Message::Data(1)).unwrap();
        input.push_back(Message::Marker("m1")).unwrap();
        input.push_back(Message::Data(2)).unwrap();

        let mut wkr = noop_waker();

        // First Input poll: drains Data(1), hits Marker → MarkerReady, stops
        let _ = input_phase.poll(&mut wkr).unwrap();

        // Poll Input again — should be blocked in MarkerReady, Data(2) stays in edge
        let _ = input_phase.poll(&mut wkr).unwrap();
        let _ = input_phase.poll(&mut wkr).unwrap();

        let _ = work_phase.poll(&mut wkr).unwrap();

        // Output drains data before marker, then forwards marker → Running
        let _ = output_phase.poll(&mut wkr).unwrap();

        // Data(1) output arrives before Marker — ordering preserved
        assert_eq!(output.poll().unwrap(), Some(Message::Data(2))); // 1*2=2
        assert_eq!(output.poll().unwrap(), Some(Message::Marker("m1")));
        // Data(2) must NOT have leaked through during the extra Input polls
        assert_eq!(output.poll().unwrap(), None);

        // Now Input resumes, drains Data(2)
        let _ = input_phase.poll(&mut wkr).unwrap();
        let _ = work_phase.poll(&mut wkr).unwrap();
        let _ = output_phase.poll(&mut wkr).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(6))); // (1+2)*2=6
    }

    #[test]
    fn flush_wakes_input_for_remaining_data() {
        let (bridge_waker, _) = test_waker();
        let input_recv = input::sync::Receiver::new(bridge_waker);
        let input = input_recv.sender();
        let output = Receiver::new();

        let input_woken = Rc::new(Cell::new(false));
        let input_waker = ThreadLocalWaker::new({
            struct Wake(Rc<Cell<bool>>);
            impl crate::connect::waker::ThreadLocalWake for Wake {
                fn wake(&self) {
                    self.0.set(true);
                }
                fn schedule_at(&self, _: std::time::Instant) {}
            }
            Wake(input_woken.clone())
        });

        let (mut input_phase, mut work_phase, mut output_phase) =
            new_phases::<usize, usize, &str, DefaultThread, _, _, _>(
                MockLine::new(noop_line_wakers()),
                input_recv,
                output.sender(),
                input_waker,
                noop_local_waker(),
                noop_local_waker(),
            );

        input.push_back(Message::Data(1)).unwrap();
        input.push_back(Message::Flush("s1")).unwrap();
        input.push_back(Message::Data(2)).unwrap();

        let mut wkr = noop_waker();

        // Input drains Data(1), hits Flush → Flushing. Data(2) still in the bridge.
        let _ = input_phase.poll(&mut wkr).unwrap();
        // Work → Ready → FlushReady
        let _ = work_phase.poll(&mut wkr).unwrap();

        assert!(!input_woken.get());

        // Output forwards Flush → Running. Must wake Input.
        let _ = output_phase.poll(&mut wkr).unwrap();

        assert!(
            input_woken.get(),
            "Output must wake Input after flush so remaining data is drained"
        );
    }
}
