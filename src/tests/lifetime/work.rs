//! Borrowed routines on the `Workable` (sync) connection trait.

use super::mock::{BorrowingBifurcation, BorrowingBiunion, BorrowingLine, LifetimeThread};
use crate::connect::sync::Receiver;
use crate::graph::Get;
use crate::marker::Unary;
use crate::thread::{ThreadBundle, ThreadStream};
use crate::work::{Connect, make_bifurcation, make_biunion, make_line};
use crate::{Closeable, Message, Trackable, bifurcation, biunion, make_push, make_work};
use std::collections::VecDeque;

// ============================================================
// line × Workable (sync, through ThreadBundle)
// ============================================================

#[test]
fn line_work_borrowed() {
    let multiplier: usize = 3;
    let mut line = make_line(BorrowingLine::new(&multiplier));

    let mut input: Box<
        dyn Closeable<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
    > = Get::get(line.as_ref()).unwrap();
    let output = Receiver::<usize, Trackable<&'static str>>::new();
    make_push(line.as_mut(), &output).unwrap();

    let mut sync_thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, LifetimeThread>(line, &mut sync_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        input.push(Message::Data(4)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(12));

        input.push(Message::Data(7)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(21));

        input.close().unwrap();
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = multiplier;
}

// ============================================================
// Multi-thread bundle sharing a single &config across two routines
// ============================================================

/// Two work lines on separate `ThreadStream`s, both borrowing the same
/// stack-allocated `&multiplier`. Cross-thread connection via `make_push`
/// (Sync edge). This is the canonical motivating use case from the
/// design doc: encoder + decoder on different threads, each holding
/// `&Model`. Exercises `'params` propagation through *two* threads in
/// the *same* `ThreadBundle`.
#[test]
fn multi_thread_shared_borrow() {
    let multiplier: usize = 3;

    let mut line_a = make_line(BorrowingLine::new(&multiplier));
    let mut line_b = make_line(BorrowingLine::new(&multiplier));

    let mut input: Box<
        dyn Closeable<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
    > = Get::get(line_a.as_ref()).unwrap();

    // line_a → line_b (cross-thread Sync push), line_b → output (external)
    make_push(line_a.as_mut(), line_b.as_ref()).unwrap();
    let output = Receiver::<usize, Trackable<&'static str>>::new();
    make_push(line_b.as_mut(), &output).unwrap();

    let mut thread_a = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, LifetimeThread>(line_a, &mut thread_a).unwrap();
    let mut thread_b = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, LifetimeThread>(line_b, &mut thread_b).unwrap();

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
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = multiplier;
}

// ============================================================
// biunion × Workable (sync, through ThreadBundle)
// ============================================================

#[test]
fn biunion_work_borrowed() {
    let bias: usize = 10;
    let mut biun = make_biunion(BorrowingBiunion {
        bias: &bias,
        out: VecDeque::new(),
    });

    let mut left: Box<
        dyn Closeable<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
    > = Get::<_, biunion::Left>::get(biun.as_ref()).unwrap();
    let mut right: Box<
        dyn Closeable<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
    > = Get::<_, biunion::Right>::get(biun.as_ref()).unwrap();
    let output = Receiver::<usize, Trackable<&'static str>>::new();
    make_push(biun.as_mut(), &output).unwrap();

    let mut sync_thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, LifetimeThread>(biun, &mut sync_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        left.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(11)); // 1 + 10

        right.push(Message::Data(2)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(20)); // 2 * 10

        left.close().unwrap();
        right.close().unwrap();
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = bias;
}

// ============================================================
// bifurcation × Workable (sync, through ThreadBundle)
// ============================================================

#[test]
fn bifurcation_work_borrowed() {
    let threshold: usize = 5;
    let mut bif = make_bifurcation(BorrowingBifurcation {
        threshold: &threshold,
        left: VecDeque::new(),
        right: VecDeque::new(),
    });

    let mut source: Box<
        dyn Closeable<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
    > = Get::get(bif.as_ref()).unwrap();
    let low_output = Receiver::<usize, Trackable<&'static str>>::new();
    let high_output = Receiver::<usize, Trackable<&'static str>>::new();
    Connect::<usize>::push::<bifurcation::Left, Unary>(bif.as_mut(), &low_output).unwrap();
    Connect::<usize>::push::<bifurcation::Right, Unary>(bif.as_mut(), &high_output).unwrap();

    let mut sync_thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, LifetimeThread>(bif, &mut sync_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        source.push(Message::Data(3)).unwrap();
        source.push(Message::Data(7)).unwrap();

        assert_eq!(low_output.read_front().unwrap(), Message::Data(3));
        assert_eq!(high_output.read_front().unwrap(), Message::Data(7));

        source.close().unwrap();
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = threshold;
}
