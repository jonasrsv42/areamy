//! Async line node that runs an [AsyncLineRoutine] driven by wakers.
//!
//! The node orchestrates the routine through a state machine:
//!
//! - **Running**: drain input → poll routine → drain output
//! - **Flushing**: poll routine until Ready → forward Flush signal → Running
//! - **Closing**: poll routine until Ready → close output → Closed
//! - **Closed**: return Ready immediately
//!
//! This guarantees the routine gets poll() cycles to complete async work
//! (e.g. I/O flush, half-close handshake) before signals are propagated.

use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::{Closeable, Pollable, Receivable, ThreadId, fatal};

/// Node state machine for managing flush and close lifecycle.
enum NodeState<SignalType> {
    /// Normal operation: drain input, poll, drain output.
    Running,
    /// Flush requested. Poll routine until Ready, then forward Flush signal.
    Flushing(SignalType),
    /// Input closed. Poll routine until Ready (cleanup), then close output.
    Closing,
    /// Terminal. Return Ready immediately on any subsequent poll.
    Closed,
}

/// Async line node. Generic over input ([Receivable]) and output ([Closeable]).
///
/// Variants by edge type:
/// - Sync→Sync: `Input = Arc<SyncBridge>`, `Output = Vec<Box<dyn Closeable + Send + Sync>>`
/// - Sync→Async: `Input = Arc<SyncBridge>`, `Output = Rc<RefCell<AsyncEdge>>`
/// - Async→Sync: `Input = Rc<RefCell<AsyncEdge>>`, `Output = Vec<Box<dyn Closeable + Send + Sync>>`
/// - Async→Async: `Input = Rc<RefCell<AsyncEdge>>`, `Output = Rc<RefCell<AsyncEdge>>`
pub struct AsyncLine<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    pub worker: RoutineType,
    pub input: InputType,
    pub output: OutputType,
    state: NodeState<SignalType>,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
    AsyncLine<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    pub fn new(worker: RoutineType, input: InputType, output: OutputType) -> Self {
        Self {
            worker,
            input,
            output,
            state: NodeState::Running,
            _thread_id: std::marker::PhantomData,
        }
    }

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
    /// Output is NOT drained here — poll_and_drain handles that.
    fn drain_input(&mut self) -> Result<(), Error> {
        loop {
            match self.input.try_recv() {
                Ok(Some(Message::Data(data))) => {
                    self.worker.send(data)?;
                }
                Ok(Some(Message::Flush(signal))) => {
                    self.worker.flush()?;
                    self.state = NodeState::Flushing(signal);
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
                        break;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Poll routine and drain any output it produced.
    fn poll_and_drain(
        &mut self,
        cx: &mut core::task::Context<'_>,
    ) -> Result<core::task::Poll<()>, Error> {
        let result = crate::Poll::poll(&mut self.worker, cx)?;
        self.drain_output()?;
        Ok(result)
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Connection
    for AsyncLine<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
}

impl<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType> Pollable
    for AsyncLine<In, Out, SignalType, ThreadIdType, RoutineType, InputType, OutputType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
    InputType: Receivable<DataType = In, SignalType = SignalType>,
    OutputType: Closeable<DataType = Out, SignalType = SignalType>,
{
    type ThreadId = ThreadIdType;

    fn poll(&mut self, cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
        if matches!(self.state, NodeState::Running) {
            self.drain_input()?;
        }

        match &self.state {
            // Terminal. Node is done, reject further polls.
            NodeState::Closed => Err(crate::closed!()),

            // Input closed. Poll routine until Ready (cleanup), then
            // close output and transition to Closed.
            NodeState::Closing => {
                let result = self.poll_and_drain(cx)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    self.output.close()?;
                    self.state = NodeState::Closed;
                    return Err(crate::closed!());
                }
                Ok(core::task::Poll::Pending)
            }

            // Flush requested. Poll routine until Ready, then forward
            // the Flush signal downstream and return to Running.
            NodeState::Flushing(_) => {
                let result = self.poll_and_drain(cx)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    let signal = match std::mem::replace(&mut self.state, NodeState::Running) {
                        NodeState::Flushing(s) => s,
                        _ => return Err(fatal!("expected Flushing state")),
                    };
                    self.push_output(Message::Flush(signal))?;
                }
                Ok(core::task::Poll::Pending)
            }

            // Normal operation. Poll routine for async work, drain output.
            // Ready here is unexpected — routine should only return Ready
            // in response to flush or close.
            NodeState::Running => {
                let result = self.poll_and_drain(cx)?;
                if matches!(result, core::task::Poll::Ready(())) {
                    return Err(fatal!(
                        "routine returned Ready without pending flush or close"
                    ));
                }
                Ok(core::task::Poll::Pending)
            }
        }
    }
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

        let mut node = AsyncLine::<usize, usize, &str, DefaultThread, _, _, _>::new(
            AsyncMockLine::new(),
            input.clone(),
            output.clone(),
        );

        input.push_back(Message::Data(2)).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        assert!(matches!(
            node.poll(&mut cx).unwrap(),
            core::task::Poll::Pending
        ));

        assert_eq!(output.poll().unwrap(), Some(Message::Data(4)));
    }

    #[test]
    fn poll_returns_closed_on_closed_input() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::<usize, &str>::new(waker));
        let output = Arc::new(SyncEdge::new());

        let mut node = AsyncLine::<usize, usize, &str, DefaultThread, _, _, _>::new(
            AsyncMockLine::new(),
            input.clone(),
            output,
        );

        SyncBridge::close(&input).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let result = node.poll(&mut cx);
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

        let mut node = AsyncLine::<usize, usize, &str, DefaultThread, _, _, _>::new(
            AsyncMockLine::new(),
            input,
            output,
        );

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = node.poll(&mut cx).unwrap();
        assert_eq!(node.worker.poll_count, 1);

        let _ = node.poll(&mut cx).unwrap();
        assert_eq!(node.worker.poll_count, 2);
    }

    #[test]
    fn poll_drains_multiple_inputs() {
        let (waker, _) = test_waker();
        let input = Arc::new(SyncBridge::new(waker));
        let output = Arc::new(SyncEdge::new());

        let mut node = AsyncLine::<usize, usize, &str, DefaultThread, _, _, _>::new(
            AsyncMockLine::new(),
            input.clone(),
            output.clone(),
        );

        input.push_back(Message::Data(1)).unwrap();
        input.push_back(Message::Data(2)).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = node.poll(&mut cx).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(2)));
        assert_eq!(output.poll().unwrap(), Some(Message::Data(6)));
    }
}
