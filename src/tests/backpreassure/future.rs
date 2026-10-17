//! Bounded [FutureRoutine] queues through the real poll loop: a stalled downstream holds the
//! pipeline to its bounds, nothing is lost, signals keep their place, and parked futures are woken
//! whichever waker they parked with.
//!
//! Every read and write polls with a deadline, so a lost wake fails instead of hanging.

use super::{Msg, Signal, data, one, push, read_n, settled, stopped};
use crate::biunion::{Left, Right};
use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::Closeable;
use crate::message::Message;
use crate::poll::future::{Input, biunion, input, line, output};
use crate::poll::{self, try_join};
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::ThreadBundle;
use crate::work::Writer;
use crate::{At, Pushes, Writes};
use alloc::sync::Arc;
use alloc::task::Wake;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

crate::thread_id!(FutureThread);

const TIMEOUT: Duration = super::TIMEOUT;
/// Output marking a segment's end, sent by the routine on Flush.
const FLUSHED: usize = 999;

/// Echoes every value; returns at Flush.
pub(super) fn echo(
    input: input::Receiver<usize>,
    output: output::Sender<usize>,
) -> Pin<Box<dyn Future<Output = Result<(), Error>>>> {
    Box::pin(async move {
        while let Input::Data(value) = input.recv().await? {
            output.send(value).await?;
        }
        Ok(())
    })
}

/// A line node bounded to one everywhere: its input edge, its routine's queues, and its output
/// edge; plus a writer into it.
fn bounded_line<F>(
    routine: F,
) -> (
    poll::Thread<'static, FutureThread>,
    Writer<'static, usize, Signal>,
    Receiver<usize, Signal>,
)
where
    F: FnMut(
            input::Receiver<usize>,
            output::Sender<usize>,
        ) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>
        + Send
        + 'static,
{
    let mut thread = poll::Thread::<FutureThread>::new();
    let mut node = thread
        .builder()
        .line()
        .bounded(one())
        .build(line::FutureRoutine::factory(routine).bounded(one()))
        .input()
        .output();
    let writer = node.writer().unwrap();
    let output = Receiver::bounded(one());
    node.pushes(&output).unwrap();
    thread.add(node);
    (thread, writer, output)
}

#[test]
fn a_stalled_bounded_line_holds_its_bounds_and_loses_nothing() {
    let _deadline = deadline(DEADLINE);
    const COUNT: usize = 100;
    const IN_FLIGHT: usize = 1 // the input edge
        + 1 // Input's held message
        + 1 // the input queue
        + 1 // the future's value waiting in `send`
        + 1 // the output queue
        + 1 // Output's held message
        + 1; // the output edge

    let (thread, mut writer, output) = bounded_line(echo);
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);
    let pushed = Arc::new(AtomicUsize::new(0));
    let counter = pushed.clone();

    thread::scope(|s| {
        let handle = bundle.start(s);
        let writing = s.spawn(move || {
            for value in 0..COUNT {
                push(&mut writer, Message::Data(value));
                counter.fetch_add(1, Ordering::SeqCst);
            }
            writer
        });

        // Nothing read: the pipeline fills to its bounds and stops.
        let filled = settled(&pushed, IN_FLIGHT);

        let received = read_n(&output, COUNT);
        // Close only once all is out: close is fast and drops what's in flight.
        writing.join().unwrap().close().unwrap();
        assert!(handle.join().errors().is_empty());
        // Asserted after teardown, so a failure doesn't leave the poll thread running.
        assert_eq!(filled, IN_FLIGHT, "pushed before stalling");
        assert_eq!(received, data(0..COUNT));
    });
}

#[test]
fn a_flush_lands_behind_sends_waiting_for_room() {
    let _deadline = deadline(DEADLINE);
    const COUNT: usize = 2;
    // Sends resolved: short of all of them once the pipeline stalls, so one waits.
    let sent = Arc::new(AtomicUsize::new(0));
    let counter = sent.clone();
    // Two outputs per input, so the future waits in `send` while Flush queues up.
    let (thread, mut writer, output) = bounded_line(
        move |input: input::Receiver<usize>, output: output::Sender<usize>| {
            let sent = counter.clone();
            Box::pin(async move {
                loop {
                    match input.recv().await? {
                        Input::Data(value) => {
                            output.send(value * 2).await?;
                            sent.fetch_add(1, Ordering::SeqCst);
                            output.send(value * 2 + 1).await?;
                            sent.fetch_add(1, Ordering::SeqCst);
                        }
                        Input::Flush => {
                            output.send(FLUSHED).await?;
                            return Ok(());
                        }
                    }
                }
            })
        },
    );
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    let pushed = Arc::new(AtomicUsize::new(0));
    let pushes = pushed.clone();

    thread::scope(|s| {
        let handle = bundle.start(s);
        let writing = s.spawn(move || {
            for value in 0..COUNT {
                push(&mut writer, Message::Data(value));
                pushes.fetch_add(1, Ordering::SeqCst);
            }
            push(&mut writer, Message::Flush("f".into()));
            pushes.fetch_add(1, Ordering::SeqCst);
            writer
        });

        // Nothing read: the Flush gets in while a send still waits for room.
        let filled = settled(&pushed, COUNT + 1);
        let sent_by_then = sent.load(Ordering::SeqCst);

        let mut expected = data(0..2 * COUNT);
        expected.push(Message::Data(FLUSHED));
        expected.push(Message::Flush("f".into()));
        let received = read_n(&output, expected.len());
        writing.join().unwrap().close().unwrap();
        assert!(handle.join().errors().is_empty());
        assert_eq!(filled, COUNT + 1, "the Flush got in");
        assert!(sent_by_then < 2 * COUNT, "no send was waiting");
        assert_eq!(received, expected);
    });
}

#[test]
fn closing_while_a_send_waits_shuts_down() {
    let _deadline = deadline(DEADLINE);
    // Sends started; it stops growing once a send waits for good.
    let sends = Arc::new(AtomicUsize::new(0));
    let counter = sends.clone();
    // Sends until stopped; the output is never read, so a send waits for good.
    let (thread, mut writer, output) = bounded_line(
        move |input: input::Receiver<usize>, output: output::Sender<usize>| {
            let started = counter.clone();
            Box::pin(async move {
                let _ = input.recv().await?;
                for value in 0.. {
                    started.fetch_add(1, Ordering::SeqCst);
                    output.send(value).await?;
                }
                Ok(())
            })
        },
    );
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    thread::scope(|s| {
        let handle = bundle.start(s);
        push(&mut writer, Message::Data(0));
        let waiting = stopped(&sends);
        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
        drop(output);
        assert!(waiting > 0, "the routine never sent");
    });
}

/// Under backpressure, a timed send times out on the runtime's timer and hands its item back; the
/// routine can still send it, and nothing is lost.
#[test]
fn a_timed_send_on_a_stalled_output_hands_its_item_back() {
    let _deadline = deadline(DEADLINE);
    // The handed-back item plus one; 0 until then.
    let handed_back = Arc::new(AtomicUsize::new(0));
    let record = handed_back.clone();
    let (thread, mut writer, output) = bounded_line(
        move |input: input::Receiver<usize>, output: output::Sender<usize>| {
            let record = record.clone();
            Box::pin(async move {
                let _ = input.recv().await?;
                let mut value = 0;
                let back = loop {
                    match output
                        .send_with_timeout(value, Duration::from_millis(20))
                        .await?
                    {
                        None => value += 1,
                        Some(back) => break back,
                    }
                };
                record.store(back + 1, Ordering::SeqCst);
                output.send(back).await?;
                // A segment ends only at a Flush.
                while let Input::Data(_) = input.recv().await? {}
                Ok(())
            })
        },
    );
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    thread::scope(|s| {
        let handle = bundle.start(s);
        push(&mut writer, Message::Data(0));
        let deadline = Instant::now() + TIMEOUT;
        while handed_back.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }

        // Everything before the handed-back item, then the item itself, sent again.
        let sent = handed_back.load(Ordering::SeqCst);
        let received = read_n(&output, sent);
        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
        // Asserted after teardown, so a failure doesn't leave the poll thread running.
        assert!(sent > 0, "no timed send handed its item back");
        assert_eq!(received, data(0..sent));
    });
}

/// A biunion bounded to one everywhere, echoing left as is and right plus 1000, through clones of
/// one output; plus a writer per side.
fn bounded_biunion() -> (
    poll::Thread<'static, FutureThread>,
    Writer<'static, usize, Signal>,
    Writer<'static, usize, Signal>,
    Receiver<usize, Signal>,
) {
    let mut thread = poll::Thread::<FutureThread>::new();
    let mut node = thread
        .builder()
        .biunion()
        .bounded::<Left>(one())
        .bounded::<Right>(one())
        .build(
            biunion::FutureRoutine::factory(
                |left: input::Receiver<usize>,
                 right: input::Receiver<usize>,
                 output: output::Sender<usize>| {
                    // Both branches send through clones of one bounded output: a pop must wake
                    // whichever waits.
                    let left_out = output.clone();
                    let right_out = output;
                    Box::pin(async move {
                        let left = async move {
                            while let Input::Data(value) = left.recv().await? {
                                left_out.send(value).await?;
                            }
                            Ok::<_, Error>(())
                        };
                        let right = async move {
                            while let Input::Data(value) = right.recv().await? {
                                right_out.send(1000 + value).await?;
                            }
                            Ok::<_, Error>(())
                        };
                        try_join(left, right).await?;
                        Ok(())
                    })
                },
            )
            .bounded(one()),
        )
        .input::<Left>()
        .input::<Right>()
        .output();
    let left_writer = node.at_ref::<Left>().writer().unwrap();
    let right_writer = node.at_ref::<Right>().writer().unwrap();
    let output = Receiver::bounded(one());
    node.pushes(&output).unwrap();
    thread.add(node);
    (thread, left_writer, right_writer, output)
}

#[test]
fn a_stalled_bounded_biunion_holds_each_side_to_its_bounds() {
    let _deadline = deadline(DEADLINE);
    const COUNT: usize = 50;
    // On the fed side.
    const IN_FLIGHT: usize = 1 // the input edge
        + 1 // Input's held message
        + 1 // the input queue
        + 1 // the future's value waiting in `send`
        + 1 // the output queue
        + 1 // Output's held message
        + 1; // the output edge

    for left in [true, false] {
        let (thread, left_writer, right_writer, output) = bounded_biunion();
        let (mut writer, mut idle) = match left {
            true => (left_writer, right_writer),
            false => (right_writer, left_writer),
        };
        let offset = if left { 0 } else { 1000 };
        let mut bundle = ThreadBundle::new();
        bundle.add(thread);
        let pushed = Arc::new(AtomicUsize::new(0));
        let counter = pushed.clone();

        thread::scope(|s| {
            let handle = bundle.start(s);
            let writing = s.spawn(move || {
                for value in 0..COUNT {
                    push(&mut writer, Message::Data(value));
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                writer
            });

            // Nothing read: the side fills to its bounds and stops.
            let filled = settled(&pushed, IN_FLIGHT);

            let received = read_n(&output, COUNT);
            writing.join().unwrap().close().unwrap();
            idle.close().unwrap();
            assert!(handle.join().errors().is_empty());
            // Asserted after teardown, so a failure doesn't leave the poll thread running.
            assert_eq!(filled, IN_FLIGHT, "pushed before stalling");
            assert_eq!(received, data(offset..offset + COUNT));
        });
    }
}

#[test]
fn a_bounded_biunion_serves_both_sides_through_one_output() {
    let _deadline = deadline(DEADLINE);
    const COUNT: usize = 30;
    let (thread, mut left_writer, mut right_writer, output) = bounded_biunion();
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    thread::scope(|s| {
        let handle = bundle.start(s);
        let left = s.spawn(move || {
            for value in 0..COUNT {
                push(&mut left_writer, Message::Data(value));
            }
            left_writer
        });
        let right = s.spawn(move || {
            for value in 0..COUNT {
                push(&mut right_writer, Message::Data(value));
            }
            right_writer
        });

        let received = read_n(&output, 2 * COUNT);
        let side = |low: usize| -> Vec<Msg> {
            received
                .iter()
                .filter(|m| matches!(m, Message::Data(v) if (low..low + COUNT).contains(v)))
                .cloned()
                .collect()
        };
        // Each side in order; together, everything.
        assert_eq!(side(0), data(0..COUNT));
        assert_eq!(side(1000), data(1000..1000 + COUNT));
        // Close only once all is out: close is fast and drops what's in flight.
        left.join().unwrap().close().unwrap();
        right.join().unwrap().close().unwrap();
        assert!(handle.join().errors().is_empty());
    });
}

/// One branch of [Unordered]: woken, it marks itself ready and wakes the parent.
struct Branch {
    ready: AtomicBool,
    parent: Mutex<Option<Waker>>,
}

impl Wake for Branch {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::SeqCst);
        if let Some(parent) = self.parent.lock().unwrap().as_ref() {
            parent.wake_by_ref();
        }
    }
}

type BranchFut = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

/// Polls two futures with a waker per branch and only the branches woken, as `FuturesUnordered`
/// does: a branch parked on a queue is re-polled only through its own waker.
struct Unordered {
    branches: Vec<(Arc<Branch>, Option<BranchFut>)>,
}

impl Unordered {
    fn new(first: BranchFut, second: BranchFut) -> Self {
        let branch = |fut| {
            let state = Arc::new(Branch {
                ready: AtomicBool::new(true),
                parent: Mutex::new(None),
            });
            (state, Some(fut))
        };
        Self {
            branches: vec![branch(first), branch(second)],
        }
    }
}

impl Future for Unordered {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for (state, slot) in &mut self.branches {
            *state.parent.lock().unwrap() = Some(cx.waker().clone());
            let Some(fut) = slot else { continue };
            if !state.ready.swap(false, Ordering::SeqCst) {
                continue;
            }
            let waker = Waker::from(state.clone());
            if let Poll::Ready(result) = fut.as_mut().poll(&mut Context::from_waker(&waker)) {
                result?;
                *slot = None;
            }
        }
        if self.branches.iter().all(|(_, slot)| slot.is_none()) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

#[test]
fn futures_parked_with_their_own_waker_are_woken_through_it() {
    let _deadline = deadline(DEADLINE);
    const COUNT: usize = 50;
    let (thread, mut writer, output) = bounded_line(
        |input: input::Receiver<usize>, output: output::Sender<usize>| {
            Box::pin(Unordered::new(
                echo(input, output),
                Box::pin(async { Ok(()) }),
            ))
        },
    );
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    thread::scope(|s| {
        let handle = bundle.start(s);
        let writing = s.spawn(move || {
            for value in 0..COUNT {
                push(&mut writer, Message::Data(value));
            }
            writer
        });

        assert_eq!(read_n(&output, COUNT), data(0..COUNT));
        writing.join().unwrap().close().unwrap();
        assert!(handle.join().errors().is_empty());
    });
}
