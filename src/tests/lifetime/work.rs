//! Borrowed routines on the `Workable` (sync) connection trait.

use super::mock::{BorrowingBifurcation, BorrowingBiunion, BorrowingLine, LifetimeThread};
use crate::connect::sync::Receiver;
use crate::marker::Unary;
use crate::thread::{ThreadBundle, ThreadStream};
use crate::work::{Connect, Source, make_bifurcation, make_biunion, make_line};
use crate::{Closeable, Message, Pushable, bifurcation, biunion, make_push, make_work};
use std::collections::VecDeque;

#[test]
fn line_work_borrowed() {
    let multiplier: usize = 3;
    let mut line = make_line(BorrowingLine::new(&multiplier));

    let mut input = Source::new(line.as_ref()).unwrap();
    let output = Receiver::new();
    make_push(line.as_mut(), &output).unwrap();

    let mut thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(line, &mut thread).unwrap();

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
/// both holding `&Model`). Cross-thread connection via `make_push`.
#[test]
fn multi_thread_shared_borrow() {
    let multiplier: usize = 3;

    let mut line_a = make_line(BorrowingLine::new(&multiplier));
    let mut line_b = make_line(BorrowingLine::new(&multiplier));

    let mut input = Source::new(line_a.as_ref()).unwrap();
    make_push(line_a.as_mut(), line_b.as_ref()).unwrap();
    let output = Receiver::new();
    make_push(line_b.as_mut(), &output).unwrap();

    let mut thread_a = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(line_a, &mut thread_a).unwrap();
    let mut thread_b = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(line_b, &mut thread_b).unwrap();

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
    let mut biun = make_biunion(BorrowingBiunion {
        bias: &bias,
        out: VecDeque::new(),
    });

    let mut left = Source::new::<biunion::Left>(biun.as_ref()).unwrap();
    let mut right = Source::new::<biunion::Right>(biun.as_ref()).unwrap();
    let output = Receiver::new();
    make_push(biun.as_mut(), &output).unwrap();

    let mut thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(biun, &mut thread).unwrap();

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
    let mut bif = make_bifurcation(BorrowingBifurcation {
        threshold: &threshold,
        left: VecDeque::new(),
        right: VecDeque::new(),
    });

    let mut source = Source::new(bif.as_ref()).unwrap();
    let low = Receiver::new();
    let high = Receiver::new();
    Connect::<usize>::push::<bifurcation::Left, Unary>(bif.as_mut(), &low).unwrap();
    Connect::<usize>::push::<bifurcation::Right, Unary>(bif.as_mut(), &high).unwrap();

    let mut thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(bif, &mut thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        source.push(Message::Data(3)).unwrap();
        source.push(Message::Data(7)).unwrap();

        assert_eq!(low.read_front().unwrap(), Message::Data(3));
        assert_eq!(high.read_front().unwrap(), Message::Data(7));

        source.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = threshold;
}
