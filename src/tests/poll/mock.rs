//! Routines shared by the poll integration tests.

use crate::Accept;
use crate::error::Error;
use crate::poll;
use crate::poll::future::{Input, input};
use crate::poll::{Context, LineWakers};
use crate::tests::mock::WakingOutput;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use core::pin::pin;
use std::sync::Mutex;

/// Routine that queues input in send via the future input queue, then
/// doubles it in poll. A send wakes Work once poll parked on the queue.
pub(super) struct PollDouble {
    input: input::Sender<usize>,
    receiver: input::Receiver<usize>,
    output: WakingOutput,
}

impl PollDouble {
    pub(super) fn new(wakers: LineWakers) -> Self {
        // The timer would re-poll the routine at a `recv_with_timeout` deadline.
        let (input, receiver) = input::channel(wakers.input, wakers.work, None);
        Self {
            input,
            receiver,
            output: WakingOutput::new(wakers.output),
        }
    }
}

impl crate::TrySend<usize> for PollDouble {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
        self.input.try_send(message)
    }
}

impl crate::Next<usize> for PollDouble {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop())
    }
}

impl crate::Flush for PollDouble {
    fn flush(&mut self) -> Result<(), Error> {
        self.input.flush()
    }
}

impl crate::Poll for PollDouble {
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        let mut std_cx = core::task::Context::from_waker(&cx.wake.sync);
        loop {
            let polled = pin!(self.receiver.recv()).poll(&mut std_cx);
            match polled {
                core::task::Poll::Ready(Ok(Input::Data(value))) => {
                    self.output.push(value * 2);
                }
                core::task::Poll::Ready(Ok(Input::Flush)) => {
                    self.receiver = self.input.renew()?;
                    return Ok(core::task::Poll::Ready(()));
                }
                core::task::Poll::Ready(Err(_)) => {
                    return Ok(core::task::Poll::Ready(()));
                }
                core::task::Poll::Pending => {
                    return Ok(core::task::Poll::Pending);
                }
            }
        }
    }
}

impl poll::LineRoutine<usize, usize> for PollDouble {}

/// Accumulates input values into a shared Vec. Used to verify sink nodes
/// actually receive and process data.
pub(super) struct PollAccumulator {
    collected: Arc<Mutex<Vec<usize>>>,
    flushed: bool,
}

impl PollAccumulator {
    pub(super) fn new(collected: Arc<Mutex<Vec<usize>>>) -> Self {
        Self {
            collected,
            flushed: false,
        }
    }
}

impl crate::TrySend<usize> for PollAccumulator {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
        self.collected.lock().unwrap().push(message);
        Ok(Accept::Accepted)
    }
}

impl crate::Next<usize> for PollAccumulator {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl crate::Flush for PollAccumulator {
    fn flush(&mut self) -> Result<(), Error> {
        self.flushed = true;
        Ok(())
    }
}

impl crate::Poll for PollAccumulator {
    fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        if self.flushed {
            self.flushed = false;
            return Ok(core::task::Poll::Ready(()));
        }
        Ok(core::task::Poll::Pending)
    }
}

impl poll::LineRoutine<usize, usize> for PollAccumulator {}

/// Sync Double — used for the sync part of the graph.
pub(super) struct Double {
    output: VecDeque<usize>,
}

impl Double {
    pub(super) fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

impl crate::Send<usize> for Double {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.push_back(message * 2);
        Ok(())
    }
}

impl crate::Next<usize> for Double {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop_front())
    }
}

impl crate::Flush for Double {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::LineRoutine<usize, usize> for Double {}

crate::thread_id!(pub(super) IoThread);
