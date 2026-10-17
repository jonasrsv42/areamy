//! Borrowed routines on the `Workable` (sync) connection trait.

use super::mock::{BorrowingBifurcation, BorrowingBiunion, BorrowingLine, LifetimeThread};
use crate::edge::sync::Receiver;
use crate::thread::ThreadBundle;
use crate::work;
use crate::{At, Closeable, Message, Pushable, Pushes, Streams, Writes, bifurcation, biunion};
use alloc::collections::VecDeque;

#[test]
fn line_work_borrowed() {
    let multiplier: usize = 3;
    let mut line = work::Line::of(BorrowingLine::new(&multiplier));

    let mut input = line.writer().unwrap();
    let output = Receiver::new();
    line.pushes(&output).unwrap();

    let thread = line.stream::<LifetimeThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        input.push(Message::Data(4)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(12));

        input.push(Message::Data(7)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(21));

        input.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}

/// Canonical motivating case: two work lines on separate threads in the
/// same bundle, both borrowing `&multiplier` (think encoder + decoder
/// both holding `&Model`). Cross-thread connection via `pushes`.
#[test]
fn multi_thread_shared_borrow() {
    let multiplier: usize = 3;

    let mut line_a = work::Line::of(BorrowingLine::new(&multiplier));
    let mut line_b = work::Line::of(BorrowingLine::new(&multiplier));

    let mut input = line_a.writer().unwrap();
    line_a.pushes(&line_b).unwrap();
    let output = Receiver::new();
    line_b.pushes(&output).unwrap();

    let thread_a = line_a.stream::<LifetimeThread>();
    let thread_b = line_b.stream::<LifetimeThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread_a);
    bundle.add(thread_b);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        // 2 → ×3 (thread_a) → 6 → ×3 (thread_b) → 18
        input.push(Message::Data(2)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(18));

        input.push(Message::Data(5)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(45));

        input.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}

#[test]
fn biunion_work_borrowed() {
    let bias: usize = 10;
    let mut biun = work::Biunion::of(BorrowingBiunion {
        bias: &bias,
        out: VecDeque::new(),
    });

    let mut left = biun.at_ref::<biunion::Left>().writer().unwrap();
    let mut right = biun.at_ref::<biunion::Right>().writer().unwrap();
    let output = Receiver::new();
    biun.pushes(&output).unwrap();

    let thread = biun.stream::<LifetimeThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        left.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(11)); // 1 + 10

        right.push(Message::Data(2)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(20)); // 2 * 10

        left.close().unwrap();
        right.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = bias;
}

#[test]
fn bifurcation_work_borrowed() {
    let threshold: usize = 5;
    let mut bif = work::Bifurcation::of(BorrowingBifurcation {
        threshold: &threshold,
        left: VecDeque::new(),
        right: VecDeque::new(),
    });

    let mut writer = bif.writer().unwrap();
    let low = Receiver::new();
    let high = Receiver::new();
    bif.at_mut::<bifurcation::Left>().pushes(&low).unwrap();
    bif.at_mut::<bifurcation::Right>().pushes(&high).unwrap();

    let thread = bif.stream::<LifetimeThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        writer.push(Message::Data(3)).unwrap();
        writer.push(Message::Data(7)).unwrap();

        assert_eq!(low.read_front().unwrap(), Message::Data(3));
        assert_eq!(high.read_front().unwrap(), Message::Data(7));

        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = threshold;
}
