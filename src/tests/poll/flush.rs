//! Flushes the routine takes several polls to finish, then close.

use crate::error::Error;
use crate::poll;
use crate::sync::Receiver;
use crate::work;
use crate::{
    Accept, Closeable, DefaultThread, Message, Pushable, Pushes, Streams, ThreadBundle, Via, Writes,
};
use alloc::sync::Arc;
use core::pin::pin;
use std::sync::Mutex;

use super::mock::{Double, IoThread, PollDouble};
use crate::poll::future::{Input, input};
use crate::poll::{Context, LineWakers};
use crate::tests::mock::WakingOutput;

/// Simulates an I/O routine with a half-close handshake on flush.
/// Uses the future input queue to wake Work on Send/Flush.
///
/// - send() queues data (wakes a parked poll)
/// - flush() queues the flush marker (wakes a parked poll)
/// - poll() drains queue, doubles data, simulates multi-cycle handshake on flush
struct HalfCloseRoutine {
    input: input::Sender<usize>,
    receiver: input::Receiver<usize>,
    output: WakingOutput,
    flush_cycles: usize,
    flush_cycles_remaining: usize,
    flush_count: Arc<Mutex<usize>>,
}

impl HalfCloseRoutine {
    fn new(wakers: LineWakers, flush_cycles: usize, flush_count: Arc<Mutex<usize>>) -> Self {
        let (input, receiver) = input::channel(wakers.input, wakers.work, wakers.timer, None);
        Self {
            input,
            receiver,
            output: WakingOutput::new(wakers.output),
            flush_cycles,
            flush_cycles_remaining: 0,
            flush_count,
        }
    }
}

impl crate::TrySend<usize> for HalfCloseRoutine {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
        self.input.try_send(message)
    }
}

impl crate::Next<usize> for HalfCloseRoutine {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop())
    }
}

impl crate::Flush for HalfCloseRoutine {
    fn flush(&mut self) -> Result<(), Error> {
        self.input.flush()
    }
}

impl crate::Poll for HalfCloseRoutine {
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        let mut std_cx = core::task::Context::from_waker(&cx.wake.sync);
        // If mid-handshake, count down
        if self.flush_cycles_remaining > 0 {
            self.flush_cycles_remaining -= 1;
            if self.flush_cycles_remaining > 0 {
                cx.wake.sync.wake_by_ref();
                return Ok(core::task::Poll::Pending);
            }
            *self.flush_count.lock().unwrap() += 1;
            return Ok(core::task::Poll::Ready(()));
        }

        // Drain queue until flush or empty
        loop {
            let polled = pin!(self.receiver.recv()).poll(&mut std_cx);
            match polled {
                core::task::Poll::Ready(Ok(Input::Data(val))) => {
                    self.output.push(val * 2);
                }
                core::task::Poll::Ready(Ok(Input::Flush)) => {
                    self.receiver = self.input.renew()?;
                    self.flush_cycles_remaining = self.flush_cycles;
                    cx.wake.sync.wake_by_ref();
                    return Ok(core::task::Poll::Pending);
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

impl poll::LineRoutine<usize, usize> for HalfCloseRoutine {}

/// Flush signal is held until the routine's poll() returns Ready.
///
/// HalfCloseRoutine takes 3 poll cycles to complete the "handshake".
/// The Flush signal only reaches the output after those 3 cycles.
/// Data sent before the flush is doubled and forwarded.
#[test]
fn flush_waits_for_routine_ready() -> Result<(), Error> {
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;

    let flush_count = Arc::new(Mutex::new(0));
    let mut async_thread = poll::Thread::<IoThread>::new();

    // HalfCloseRoutine needs 3 poll cycles to complete flush handshake
    let parent = async_thread
        .line({
            let fc = flush_count.clone();
            move |w| HalfCloseRoutine::new(w, 3, fc)
        })
        .input();
    writer_node.pushes(&parent)?;

    // Child collects output to verify flush ordering
    let mut child = parent.via(async_thread.line(PollDouble::new))?.output();

    let output = Receiver::new();
    child.pushes(&output)?;

    async_thread.add(child);

    let sync_thread = writer_node.stream::<DefaultThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // Send data then flush
        // 5 → Double → 10 → HalfCloseRoutine.send(10) → pending=[10]
        writer.push(Message::Data(5))?;

        // Flush: HalfCloseRoutine.flush() → output=[20], starts 3-cycle handshake
        // After 3 poll cycles: Ready → Flush signal forwarded → child receives it
        writer.push(Message::Flush("segment-1".into()))?;

        // Data(20) arrives at child (PollDouble) → 40
        assert_eq!(output.read_front()?, Message::Data(40));

        // Flush signal arrives at child after handshake completes
        assert_eq!(output.read_front()?, Message::Flush("segment-1".into()));

        // Verify handshake completed exactly once
        assert_eq!(*flush_count.lock().unwrap(), 1);

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Accumulates data until flush, then outputs the sum. Simulates a
/// routine that batches work and only emits results on flush boundaries.
/// Takes `flush_cycles` poll cycles to complete each flush (I/O simulation).
struct BatchRoutine {
    accumulator: usize,
    output: WakingOutput,
    flush_requested: bool,
    flush_cycles: usize,
    flush_cycles_remaining: usize,
}

impl BatchRoutine {
    fn new(wakers: LineWakers, flush_cycles: usize) -> Self {
        Self {
            accumulator: 0,
            output: WakingOutput::new(wakers.output),
            flush_requested: false,
            flush_cycles,
            flush_cycles_remaining: 0,
        }
    }
}

impl crate::TrySend<usize> for BatchRoutine {
    fn try_send(&mut self, message: usize) -> Result<Accept<usize>, Error> {
        self.accumulator += message;
        Ok(Accept::Accepted)
    }
}

impl crate::Next<usize> for BatchRoutine {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop())
    }
}

impl crate::Flush for BatchRoutine {
    fn flush(&mut self) -> Result<(), Error> {
        if self.accumulator > 0 {
            self.output.push(self.accumulator);
        }
        self.accumulator = 0;
        self.flush_requested = true;
        self.flush_cycles_remaining = self.flush_cycles;
        Ok(())
    }
}

impl crate::Poll for BatchRoutine {
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        if self.flush_requested {
            if self.flush_cycles_remaining == 0 {
                self.flush_requested = false;
                return Ok(core::task::Poll::Ready(()));
            }
            self.flush_cycles_remaining -= 1;
            cx.wake.sync.wake_by_ref();
        }
        Ok(core::task::Poll::Pending)
    }
}

impl poll::LineRoutine<usize, usize> for BatchRoutine {}

/// Multiple flushes then close. BatchRoutine accumulates data between
/// flushes and emits the sum on each flush boundary.
///
/// ```text
/// writer → Double → BatchRoutine(2 cycles) → PollDouble → output
/// ```
///
/// Sequence:
///   Data(1), Data(2), Flush("s1")  → batch emits 6 (2+4), PollDouble → 12
///   Data(3), Flush("s2")           → batch emits 6, PollDouble → 12
///   Close                          → empty flush, close propagates
#[test]
fn multi_flush_then_close() -> Result<(), Error> {
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let parent = async_thread.line(|w| BatchRoutine::new(w, 2)).input();
    writer_node.pushes(&parent)?;

    let mut child = parent.via(async_thread.line(PollDouble::new))?.output();

    let output = Receiver::new();
    child.pushes(&output)?;

    async_thread.add(child);

    let sync_thread = writer_node.stream::<DefaultThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // Segment 1: Data(1), Data(2), Flush
        // Double: 1→2, 2→4. Batch accumulates 2+4=6.
        // Flush: emits 6, 2-cycle handshake, Flush("s1") forwarded.
        // PollDouble: 6→12
        writer.push(Message::Data(1))?;
        writer.push(Message::Data(2))?;
        writer.push(Message::Flush("s1".into()))?;

        assert_eq!(output.read_front()?, Message::Data(12));
        assert_eq!(output.read_front()?, Message::Flush("s1".into()));

        // Segment 2: Data(3), Flush
        // Double: 3→6. Batch accumulates 6.
        // Flush: emits 6, 2-cycle handshake, Flush("s2") forwarded.
        // PollDouble: 6→12
        writer.push(Message::Data(3))?;
        writer.push(Message::Flush("s2".into()))?;

        assert_eq!(output.read_front()?, Message::Data(12));
        assert_eq!(output.read_front()?, Message::Flush("s2".into()));

        // Close: flush() called (accumulator=0, nothing emitted).
        // poll() returns Ready → close propagates.
        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}
