//! One pipeline through every kind of scheduling: a pull chain run by a work thread, a poll
//! `FutureRoutine` node, then another work thread. A stalled sink stops the whole chain, back to
//! the writer, and nothing is lost.

use super::future::echo;
use super::{Msg, Signal, data, one, push, read_n, settled};
use crate::edge::sync::Receiver;
use crate::graph::Closeable;
use crate::message::Message;
use crate::node::line::routine::tests::MockWaitLine;
use crate::poll;
use crate::poll::future::line::FutureRoutine;
use crate::pull::{Pullable, WriterBuffer};
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::ThreadBundle;
use crate::work::{self, Line, Writer};
use crate::{Pushes, Streams, Via, Writes};
use alloc::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

crate::thread_id!(PullStage, PollStage, WorkStage);

#[test]
fn a_stalled_sink_stops_pull_work_and_poll_stages_back_to_the_writer() {
    let _deadline = deadline(DEADLINE);
    const COUNT: usize = 200;
    // The first work line's input stays empty: it runs its `Pulled` parent only on an empty
    // input, so `Pulled` is never refused and holds nothing.
    const IN_FLIGHT: usize = 1 // the buffer
        + 1 // the first work line, blocked mid-push
        + 2 // the poll line's input edge, and Input's held message
        + 1 // the input queue
        + 1 // the future waiting in `send`
        + 2 // the output queue, and Output's parked message
        + 2 // the last work line's input edge, and itself blocked mid-push
        + 1; // the sink

    // Pull stage, run by the first work thread: writer → bounded buffer → pull line.
    let buffer = WriterBuffer::<usize, Signal, PullStage>::bounded(one());
    let mut writer: Writer<'static, usize, Signal> = buffer.writer().unwrap();
    let pulled = work::Pulled::of(buffer.then(MockWaitLine::new(1)));
    let mut first = pulled
        .via(Line::builder().bounded(one()).build(MockWaitLine::new(1)))
        .unwrap();

    // Poll stage: bounded input edge, routine queues and output.
    let mut poll_thread = poll::Thread::<PollStage>::new();
    let mut middle = poll_thread
        .builder()
        .line()
        .bounded(one())
        .build(FutureRoutine::factory(echo).bounded(one()))
        .input()
        .output();
    first.pushes(&middle).unwrap();

    // Second work stage into a bounded sink that isn't read until the chain has stalled.
    let mut last = Line::builder().bounded(one()).build(MockWaitLine::new(1));
    middle.pushes(&last).unwrap();
    let sink = Receiver::bounded(one());
    last.pushes(&sink).unwrap();
    poll_thread.add(middle);

    let mut bundle = ThreadBundle::new();
    bundle
        .add(first.stream::<PullStage>())
        .add(poll_thread)
        .add(last.stream::<WorkStage>());
    let pushed = Arc::new(AtomicUsize::new(0));
    let counter = pushed.clone();

    thread::scope(|s| {
        let handle = bundle.start(s);
        let writing = s.spawn(move || {
            for value in 0..COUNT {
                push(&mut writer, Message::Data(value));
                counter.fetch_add(1, Ordering::SeqCst);
            }
            push(&mut writer, Message::Flush("end".into()));
            writer
        });

        // Nothing read: every stage fills to its bounds and the writer stops.
        let filled = settled(&pushed, IN_FLIGHT);

        let received = read_n(&sink, COUNT + 1);
        // Close only once all is out: close is fast and drops what's in flight.
        writing.join().unwrap().close().unwrap();
        assert!(handle.join().errors().is_empty());
        // Asserted after teardown, so a failure doesn't leave the threads running.
        assert_eq!(filled, IN_FLIGHT, "pushed before stalling");
        let mut expected: Vec<Msg> = data(0..COUNT);
        expected.push(Message::Flush("end".into()));
        assert_eq!(received, expected);
    });
}
