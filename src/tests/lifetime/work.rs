//! Borrowed routines on the `Workable` (sync) connection trait.

use super::mock::{BorrowingBifurcation, BorrowingBiunion, BorrowingLine, LifetimeThread};
use crate::edge::sync::Receiver;
use crate::thread::ThreadBundle;
use crate::work::{self, ThreadStream, Writer};
use crate::{At, Closeable, Message, Push, Pushable, bifurcation, biunion};
use std::collections::VecDeque;

#[test]
fn line_work_borrowed() {
    let multiplier: usize = 3;
    let mut line = work::Line::of(BorrowingLine::new(&multiplier));

    let mut input = Writer::new(&line).unwrap();
    let output = Receiver::new();
    Push::connect(&mut line, &output).unwrap();

    let thread = ThreadStream::<LifetimeThread>::of(line);

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
/// both holding `&Model`). Cross-thread connection via `Push::connect`.
#[test]
fn multi_thread_shared_borrow() {
    let multiplier: usize = 3;

    let mut line_a = work::Line::of(BorrowingLine::new(&multiplier));
    let mut line_b = work::Line::of(BorrowingLine::new(&multiplier));

    let mut input = Writer::new(&line_a).unwrap();
    Push::connect(&mut line_a, &line_b).unwrap();
    let output = Receiver::new();
    Push::connect(&mut line_b, &output).unwrap();

    let thread_a = ThreadStream::<LifetimeThread>::of(line_a);
    let thread_b = ThreadStream::<LifetimeThread>::of(line_b);

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

    let mut left = Writer::new(&biun.at::<biunion::Left>()).unwrap();
    let mut right = Writer::new(&biun.at::<biunion::Right>()).unwrap();
    let output = Receiver::new();
    Push::connect(&mut biun, &output).unwrap();

    let thread = ThreadStream::<LifetimeThread>::of(biun);

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

    let mut writer = Writer::new(&bif).unwrap();
    let low = Receiver::new();
    let high = Receiver::new();
    Push::connect(&mut bif.at::<bifurcation::Left>(), &low).unwrap();
    Push::connect(&mut bif.at::<bifurcation::Right>(), &high).unwrap();

    let thread = ThreadStream::<LifetimeThread>::of(bif);

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
