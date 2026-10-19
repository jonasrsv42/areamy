//! Backpressure in a work graph: a slow consumer blocks its producer.

use crate::error::ErrorKind;
use crate::graph::Closeable;
use crate::message::Message;
use crate::node::bifurcation::routine::tests::HoldBifurcation;
use crate::node::biunion::routine::tests::HoldBiunion;
use crate::node::line::routine::tests::MockWaitLine;
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::Join;
use crate::work::Writer;
use crate::work::{Bifurcation, Biunion, Line};
use crate::{At, Pushes, Reads, Streams, Writes, bifurcation, biunion};
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use super::{Signal, one, push, settled};

crate::thread_id!(Producer);
crate::thread_id!(LeftProducer);
crate::thread_id!(RightProducer);

const BOUND: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Push `0..count` with `offset`, counting each into `pushed`, then hand the writer back.
fn write(
    mut writer: Writer<'static, usize, Signal>,
    offset: usize,
    count: usize,
    pushed: &AtomicUsize,
) -> Writer<'static, usize, Signal> {
    for value in offset..offset + count {
        push(&mut writer, Message::Data(value));
        pushed.fetch_add(1, Ordering::SeqCst);
    }
    writer
}

#[test]
fn slow_consumer_blocks_the_producer_thread() {
    let _deadline = deadline(DEADLINE);
    const BURST: usize = 50;
    const IN_FLIGHT: usize = 1 // the source's input edge
        + 1 // the source, blocked mid-push
        + BOUND.get(); // the sink's input edge
    // Producer thread: source → Push side path → bounded sink, read on this thread.
    let mut source = Line::builder().bounded(one()).build(MockWaitLine::new(1));
    let writer = source.writer().unwrap();
    let sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    source.pushes(&sink).unwrap();
    let mut reader = sink.reader().unwrap();
    let pushed = AtomicUsize::new(0);

    thread::scope(|scope| {
        let producer = source.stream::<Producer>().start(scope);
        let writing = scope.spawn(|| write(writer, 0, BURST, &pushed));

        // Nothing read: the sink fills, the source's push is refused `Full`, then `Stuck`, and
        // the producer thread blocks. Run with `--features trace -- --nocapture` to see "blocked" / "unblocked".
        let filled = settled(&pushed, IN_FLIGHT);

        for value in 0..BURST {
            assert_eq!(reader.read().unwrap(), Message::Data(value));
        }
        writing.join().unwrap().close().unwrap();
        // The input's close follows the burst through the side path.
        assert!(matches!(reader.read().unwrap_err().kind, ErrorKind::Closed));
        assert!(matches!(producer.join(), Join::Ok));
        assert_eq!(filled, IN_FLIGHT, "pushed before stalling");
    });
}

#[test]
fn slow_biunion_blocks_producers_on_both_bounded_inputs() {
    let _deadline = deadline(DEADLINE);
    const BURST: usize = 25;
    const RIGHT: usize = 100;
    const SIDE_BOUND: NonZeroUsize = NonZeroUsize::new(2).unwrap();
    // Per side.
    const IN_FLIGHT: usize = 1 // the source's input edge
        + 1 // the source, blocked mid-push
        + SIDE_BOUND.get(); // the biunion's input edge
    // Two producer threads, each: source → Push side path → one bounded biunion input.
    let mut to_left_source = Line::builder().bounded(one()).build(MockWaitLine::new(1));
    let mut to_right_source = Line::builder().bounded(one()).build(MockWaitLine::new(1));
    let left_writer = to_left_source.writer().unwrap();
    let right_writer = to_right_source.writer().unwrap();
    let biunion = Biunion::builder()
        .bounded::<biunion::Left>(SIDE_BOUND)
        .bounded::<biunion::Right>(SIDE_BOUND)
        .build(HoldBiunion::new(1));
    to_left_source
        .pushes(&biunion.at_ref::<biunion::Left>())
        .unwrap();
    to_right_source
        .pushes(&biunion.at_ref::<biunion::Right>())
        .unwrap();
    let mut reader = biunion.reader().unwrap();
    let (left_pushed, right_pushed) = (AtomicUsize::new(0), AtomicUsize::new(0));

    thread::scope(|scope| {
        let left = to_left_source.stream::<LeftProducer>().start(scope);
        let right = to_right_source.stream::<RightProducer>().start(scope);
        let left_writing = scope.spawn(|| write(left_writer, 0, BURST, &left_pushed));
        let right_writing = scope.spawn(|| write(right_writer, RIGHT, BURST, &right_pushed));

        // Nothing read: both biunion inputs fill and both producer threads block on them; run
        // with `--features trace -- --nocapture` to see "blocked" / "unblocked".
        let left_filled = settled(&left_pushed, IN_FLIGHT);
        let right_filled = settled(&right_pushed, IN_FLIGHT);

        let mut got = Vec::new();
        for _ in 0..2 * BURST {
            got.push(reader.read().unwrap().data().unwrap());
        }
        // Each source arrives complete and in order; how they interleave is up to timing.
        let (from_left, from_right): (Vec<usize>, Vec<usize>) =
            got.into_iter().partition(|value| *value < RIGHT);
        assert_eq!(from_left, (0..BURST).collect::<Vec<_>>());
        assert_eq!(from_right, (RIGHT..RIGHT + BURST).collect::<Vec<_>>());

        // Done reading: dropping the biunion closes its inputs, and closing the writers ends
        // the producers once they find their input empty.
        drop(reader);
        left_writing.join().unwrap().close().unwrap();
        right_writing.join().unwrap().close().unwrap();
        assert!(matches!(left.join(), Join::Ok));
        assert!(matches!(right.join(), Join::Ok));
        assert_eq!(left_filled, IN_FLIGHT, "left pushed before stalling");
        assert_eq!(right_filled, IN_FLIGHT, "right pushed before stalling");
    });
}

#[test]
fn slow_consumers_block_a_producing_bifurcation() {
    let _deadline = deadline(DEADLINE);
    const BURST: usize = 25;
    const IN_FLIGHT: usize = 1 // its input edge
        + 1 // itself, blocked mid-push
        + BOUND.get(); // each sink's input edge, both holding the same messages
    // Producer thread: bifurcation → a Push side path per side → a bounded sink per side.
    let mut bifurcation = Bifurcation::builder()
        .bounded(one())
        .build(HoldBifurcation::new(1));
    let writer = bifurcation.writer().unwrap();
    let left_sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    let right_sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    bifurcation
        .at_mut::<bifurcation::Left>()
        .pushes(&left_sink)
        .unwrap();
    bifurcation
        .at_mut::<bifurcation::Right>()
        .pushes(&right_sink)
        .unwrap();
    let mut left = left_sink.reader().unwrap();
    let mut right = right_sink.reader().unwrap();
    let pushed = AtomicUsize::new(0);

    thread::scope(|scope| {
        let producer = bifurcation.stream::<Producer>().start(scope);
        let writing = scope.spawn(|| write(writer, 0, BURST, &pushed));

        // Nothing read: both sinks fill and the producer blocks on one; run with
        // `--features trace -- --nocapture` to see "blocked" / "unblocked".
        let filled = settled(&pushed, IN_FLIGHT);

        for value in 0..BURST {
            assert_eq!(left.read().unwrap(), Message::Data(value));
            assert_eq!(right.read().unwrap(), Message::Data(value));
        }
        writing.join().unwrap().close().unwrap();
        // The input's close follows the burst out of both sides.
        assert!(matches!(left.read().unwrap_err().kind, ErrorKind::Closed));
        assert!(matches!(right.read().unwrap_err().kind, ErrorKind::Closed));
        assert!(matches!(producer.join(), Join::Ok));
        assert_eq!(filled, IN_FLIGHT, "pushed before stalling");
    });
}
