//! Backpressure in a work graph: a slow consumer blocks its producer.

use crate::At;
use crate::Push;
use crate::error::ErrorKind;
use crate::graph::{Closeable, Pushable};
use crate::message::Message;
use crate::node::bifurcation::routine::tests::HoldBifurcation;
use crate::node::biunion::routine::tests::HoldBiunion;
use crate::node::line::routine::tests::MockWaitLine;
use crate::thread::Join;
use crate::work::{Bifurcation, Biunion, Line, Reader, ThreadStream, Writer};
use crate::{bifurcation, biunion};
use std::num::NonZeroUsize;
use std::thread;
use std::time::Duration;

crate::thread_id!(Producer);
crate::thread_id!(LeftProducer);
crate::thread_id!(RightProducer);

const BOUND: NonZeroUsize = NonZeroUsize::new(4).unwrap();

#[test]
fn slow_consumer_blocks_the_producer_thread() {
    const BURST: usize = 50;
    // Producer thread: source → Push side path → bounded sink, read slowly on this thread.
    let mut source = Line::of(MockWaitLine::new(1));
    let mut writer = Writer::new(&source).unwrap();
    let sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    Push::connect(&mut source, &sink).unwrap();
    let mut reader = Reader::new(sink).unwrap();

    // The whole burst is queued up front; closing lets the producer thread end once it's out.
    for value in 0..BURST {
        writer.push(Message::Data(value)).unwrap();
    }
    writer.close().unwrap();

    thread::scope(|scope| {
        let producer = ThreadStream::<Producer>::of(source).start(scope);
        for value in 0..BURST {
            // Slower than the producer: the sink fills, the source's push is refused `Full`,
            // then `Stuck`, and the producer thread blocks. Run with `--nocapture` to see the
            // "blocked" / "unblocked" pairs.
            thread::sleep(Duration::from_millis(1));
            assert_eq!(reader.read().unwrap(), Message::Data(value));
        }
        // The input's close follows the burst through the blocked side path.
        assert!(matches!(reader.read().unwrap_err().kind, ErrorKind::Closed));
        assert!(matches!(producer.join(), Join::Ok));
    });
}

#[test]
fn slow_biunion_blocks_producers_on_both_bounded_inputs() {
    const BURST: usize = 25;
    const RIGHT: usize = 100;
    // Two producer threads, each: source → Push side path → one bounded biunion input.
    let mut to_left_source = Line::of(MockWaitLine::new(1));
    let mut to_right_source = Line::of(MockWaitLine::new(1));
    let mut left_writer = Writer::new(&to_left_source).unwrap();
    let mut right_writer = Writer::new(&to_right_source).unwrap();
    let mut biunion = Biunion::builder()
        .bounded::<biunion::Left>(NonZeroUsize::new(2).unwrap())
        .bounded::<biunion::Right>(NonZeroUsize::new(2).unwrap())
        .build(HoldBiunion::new(1));
    Push::connect(&mut to_left_source, &biunion.at::<biunion::Left>()).unwrap();
    Push::connect(&mut to_right_source, &biunion.at::<biunion::Right>()).unwrap();
    let mut reader = Reader::new(biunion).unwrap();

    for value in 0..BURST {
        left_writer.push(Message::Data(value)).unwrap();
        right_writer.push(Message::Data(RIGHT + value)).unwrap();
    }

    thread::scope(|scope| {
        let left = ThreadStream::<LeftProducer>::of(to_left_source).start(scope);
        let right = ThreadStream::<RightProducer>::of(to_right_source).start(scope);
        // The slow reader keeps both biunion inputs full, so both producer threads block on
        // them; run with `--nocapture` to see the "blocked" / "unblocked" pairs.
        let mut got = Vec::new();
        for _ in 0..2 * BURST {
            thread::sleep(Duration::from_millis(1));
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
        left_writer.close().unwrap();
        right_writer.close().unwrap();
        assert!(matches!(left.join(), Join::Ok));
        assert!(matches!(right.join(), Join::Ok));
    });
}

#[test]
fn slow_consumers_block_a_producing_bifurcation() {
    const BURST: usize = 25;
    // Producer thread: bifurcation → a Push side path per side → a bounded sink per side.
    let mut bifurcation = Bifurcation::of(HoldBifurcation::new(1));
    let mut writer = Writer::new(&bifurcation).unwrap();
    let left_sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    let right_sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    Push::connect(&mut bifurcation.at::<bifurcation::Left>(), &left_sink).unwrap();
    Push::connect(&mut bifurcation.at::<bifurcation::Right>(), &right_sink).unwrap();
    let mut left = Reader::new(left_sink).unwrap();
    let mut right = Reader::new(right_sink).unwrap();

    // Every input goes to both sides; closing lets the producer thread end once it's out.
    for value in 0..BURST {
        writer.push(Message::Data(value)).unwrap();
    }
    writer.close().unwrap();

    thread::scope(|scope| {
        let producer = ThreadStream::<Producer>::of(bifurcation).start(scope);
        // The slow readers keep both sinks full, so the producer blocks on whichever side is;
        // run with `--nocapture` to see the "blocked" / "unblocked" pairs.
        for value in 0..BURST {
            thread::sleep(Duration::from_millis(1));
            assert_eq!(left.read().unwrap(), Message::Data(value));
            assert_eq!(right.read().unwrap(), Message::Data(value));
        }
        // The input's close follows the burst out of both sides.
        assert!(matches!(left.read().unwrap_err().kind, ErrorKind::Closed));
        assert!(matches!(right.read().unwrap_err().kind, ErrorKind::Closed));
        assert!(matches!(producer.join(), Join::Ok));
    });
}
