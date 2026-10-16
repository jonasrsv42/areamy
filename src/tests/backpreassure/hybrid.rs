//! One pipeline through every kind of scheduling: a pull chain run by a work thread, a poll
//! `FutureRoutine` node, then another work thread. A stalled sink stops the whole chain, back to
//! the writer, and nothing is lost.

use super::future::echo;
use super::{Msg, Signal, TIMEOUT, data, one, push, read_n};
use crate::edge::sync::Receiver;
use crate::graph::Closeable;
use crate::message::Message;
use crate::node::line::routine::tests::MockWaitLine;
use crate::poll::future::line::FutureRoutine;
use crate::poll::{self, Sync};
use crate::pull::{Pullable, WriterBuffer};
use crate::thread::ThreadBundle;
use crate::work::{self, Line, ThreadStream, Writer};
use crate::Push;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

crate::thread_id!(PullStage, PollStage, WorkStage);

/// `count` once it holds still for 50ms: the pipeline has stalled.
fn settled(count: &AtomicUsize) -> usize {
    let deadline = Instant::now() + TIMEOUT;
    let mut last = count.load(Ordering::SeqCst);
    loop {
        thread::sleep(Duration::from_millis(50));
        let now = count.load(Ordering::SeqCst);
        if now == last {
            return now;
        }
        assert!(Instant::now() < deadline, "never stalled");
        last = now;
    }
}

#[test]
fn a_stalled_sink_stops_pull_work_and_poll_stages_back_to_the_writer() {
    const COUNT: usize = 200;
    // One message per slot, front to back:
    // - the buffer;
    // - the first work line, blocked mid-push (its input stays empty: it runs its `Pulled`
    //   parent only on an empty input, so `Pulled` is never refused and holds nothing);
    // - the poll line: its input edge, Input's held message, the input queue, the future
    //   waiting in `send`, the output queue, Output's parked message;
    // - the last work line: its input edge, and itself blocked mid-push;
    // - the sink.
    const IN_FLIGHT: usize = 11;

    // Pull stage, run by the first work thread: writer → bounded buffer → pull line.
    let buffer = WriterBuffer::<usize, Signal, PullStage>::bounded(one());
    let mut writer: Writer<'static, usize, Signal> = Writer::new(&buffer).unwrap();
    let pulled = work::Pulled::of(buffer.then(MockWaitLine::new(1)));
    let mut first = Line::builder().bounded(one()).build(MockWaitLine::new(1));
    work::Bidi::connect(pulled, &mut first).unwrap();

    // Poll stage: bounded input edge, routine queues and output.
    let mut poll_thread = poll::Thread::<'_, PollStage>::new();
    let mut middle = poll_thread
        .builder()
        .line()
        .bounded(one())
        .build(FutureRoutine::factory(echo).bounded(one()))
        .input::<Sync>()
        .output::<Sync>();
    Push::connect(&mut first, &middle).unwrap();

    // Second work stage into a bounded sink that isn't read until the chain has stalled.
    let mut last = Line::builder().bounded(one()).build(MockWaitLine::new(1));
    Push::connect(&mut middle, &last).unwrap();
    let sink = Receiver::bounded(one());
    Push::connect(&mut last, &sink).unwrap();
    poll_thread.add(middle);

    let mut bundle = ThreadBundle::new();
    bundle
        .add(ThreadStream::<PullStage>::of(first))
        .add(poll_thread)
        .add(ThreadStream::<WorkStage>::of(last));
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
        let filled = settled(&pushed);

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
