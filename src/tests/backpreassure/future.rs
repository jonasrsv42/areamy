//! Bounded [FutureRoutine] queues through the real poll loop: a stalled downstream holds the
//! pipeline to its bounds, nothing is lost, signals keep their place, and parked futures are woken
//! whichever waker they parked with.
//!
//! Every read and write polls with a deadline, so a lost wake fails instead of hanging.

use super::{Msg, Signal, data, one, push, read_all, read_n};
use crate::biunion::{Left, Right};
use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::Closeable;
use crate::message::Message;
use crate::poll::future::{Input, biunion, input, line, output};
use crate::poll::{self, Sync, try_join};
use crate::thread::ThreadBundle;
use crate::work::Writer;
use crate::{At, Push};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

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
    let mut thread = poll::Thread::<'_, FutureThread>::new();
    let mut node = thread
        .builder()
        .line()
        .bounded(one())
        .build(line::FutureRoutine::factory(routine).bounded(one()))
        .input::<Sync>()
        .output::<Sync>();
    let writer = Writer::new(&node).unwrap();
    let output = Receiver::bounded(one());
    Push::connect(&mut node, &output).unwrap();
    thread.add(node);
    (thread, writer, output)
}

#[test]
fn a_stalled_bounded_line_holds_its_bounds_and_loses_nothing() {
    const COUNT: usize = 100;
    // Input edge, Input's held message, input queue, the future's held value, output queue,
    // Output's held message, output edge.
    const IN_FLIGHT: usize = 7;

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
        thread::sleep(Duration::from_millis(100));
        let filled = pushed.load(Ordering::SeqCst);

        let received = read_n(&output, COUNT);
        // Close only once all is out: close is fast and drops what's in flight.
        writing.join().unwrap().close().unwrap();
        assert!(handle.join().errors().is_empty());
        // Asserted after teardown, so a failure doesn't leave the poll thread running.
        assert!(
            filled <= IN_FLIGHT,
            "{filled} pushed past {IN_FLIGHT} slots"
        );
        assert_eq!(received, data(0..COUNT));
    });
}

#[test]
fn a_flush_lands_behind_sends_waiting_for_room() {
    const COUNT: usize = 20;
    // Two outputs per input, so the future keeps waiting in `send` while Flush queues up.
    let (thread, mut writer, output) = bounded_line(
        |input: input::Receiver<usize>, output: output::Sender<usize>| {
            Box::pin(async move {
                loop {
                    match input.recv().await? {
                        Input::Data(value) => {
                            output.send(value * 2).await?;
                            output.send(value * 2 + 1).await?;
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

    thread::scope(|s| {
        let handle = bundle.start(s);
        s.spawn(move || {
            for value in 0..COUNT {
                push(&mut writer, Message::Data(value));
            }
            push(&mut writer, Message::Flush("f".into()));
            writer.close().unwrap();
        });

        let mut expected = data(0..2 * COUNT);
        expected.push(Message::Data(FLUSHED));
        expected.push(Message::Flush("f".into()));
        assert_eq!(read_all(&output), expected);
        assert!(handle.join().errors().is_empty());
    });
}

#[test]
fn closing_while_a_send_waits_shuts_down() {
    let parked = Arc::new(AtomicBool::new(false));
    let flag = parked.clone();
    // Sends until stopped; the output is never read, so a send waits for good.
    let (thread, mut writer, output) = bounded_line(
        move |input: input::Receiver<usize>, output: output::Sender<usize>| {
            let flag = flag.clone();
            Box::pin(async move {
                let _ = input.recv().await?;
                for value in 0.. {
                    // Output edge, Output's held message and the output queue take 0..3: the
                    // send of 3 waits.
                    flag.store(value >= 3, Ordering::SeqCst);
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
        let deadline = Instant::now() + TIMEOUT;
        while !parked.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "the routine never filled its output"
            );
            thread::sleep(Duration::from_millis(1));
        }
        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
        drop(output);
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
    let mut thread = poll::Thread::<'_, FutureThread>::new();
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
        .input::<Left, Sync>()
        .input::<Right, Sync>()
        .output::<Sync>();
    let left_writer = Writer::new(&node.at::<Left>()).unwrap();
    let right_writer = Writer::new(&node.at::<Right>()).unwrap();
    let output = Receiver::bounded(one());
    Push::connect(&mut node, &output).unwrap();
    thread.add(node);
    (thread, left_writer, right_writer, output)
}

#[test]
fn a_stalled_bounded_biunion_holds_each_side_to_its_bounds() {
    const COUNT: usize = 50;
    // The side's input edge, Input's held message and input queue, the future's held value, the
    // output queue, Output's held message, the output edge.
    const IN_FLIGHT: usize = 7;

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
            thread::sleep(Duration::from_millis(100));
            let filled = pushed.load(Ordering::SeqCst);

            let received = read_n(&output, COUNT);
            writing.join().unwrap().close().unwrap();
            idle.close().unwrap();
            assert!(handle.join().errors().is_empty());
            // Asserted after teardown, so a failure doesn't leave the poll thread running.
            assert!(
                filled <= IN_FLIGHT,
                "{filled} pushed past {IN_FLIGHT} slots"
            );
            assert_eq!(received, data(offset..offset + COUNT));
        });
    }
}

#[test]
fn a_bounded_biunion_serves_both_sides_through_one_output() {
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
