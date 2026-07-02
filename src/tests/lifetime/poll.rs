//! Borrowed routines on the `Pollable` (async) connection trait.

use super::mock::{
    BorrowingLine, BorrowingSink, LifetimePollThread, PollBorrowingBiunion, PollBorrowingLine,
    box_borrowing_sink,
};
use crate::connect::sync::Receiver;
use crate::graph::{Add, Get};
use crate::marker::Unary;
use crate::poll;
use crate::poll::future::OutputQueue;
use crate::thread::{ThreadBundle, ThreadStream};
use crate::work::{Writer, make_line};
use crate::{Closeable, Message, Pushable, biunion, make_push, make_work};

#[test]
fn line_poll_borrowed() {
    let multiplier: usize = 5;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let multiplier_ref = &multiplier;
    let mut node = thread
        .line(move |w: poll::LineWakers| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w.output),
        })
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let mut input = Writer::new(&node).unwrap();
    let output = Receiver::new();
    make_push(&mut node, &output).unwrap();

    thread.add(node);

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        input.push(Message::Data(2)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(10));

        input.push(Message::Data(3)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(15));

        input.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}

#[test]
fn biunion_poll_borrowed() {
    let bias: usize = 7;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let bias_ref = &bias;
    let mut node = thread
        .biunion(move |w: poll::BiunionWakers| PollBorrowingBiunion {
            bias: bias_ref,
            output: OutputQueue::new(w.output),
        })
        .input::<biunion::Left, poll::Sync>()
        .input::<biunion::Right, poll::Sync>()
        .output::<poll::Sync>();

    let mut left = Writer::new::<biunion::Left>(&node).unwrap();
    let mut right = Writer::new::<biunion::Right>(&node).unwrap();
    let output = Receiver::new();
    make_push(&mut node, &output).unwrap();

    thread.add(node);

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        left.push(Message::Data(3)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(10)); // 3 + 7

        right.push(Message::Data(2)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(14)); // 2 * 7

        left.close().unwrap();
        right.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = bias;
}

/// Two poll line nodes wired with `.parent()` — node `b` consumes `a` as
/// an `AsyncParent<'params>` (in-thread, no Mutex). Both routines borrow
/// `&multiplier`.
#[test]
fn poll_async_parent_chain_borrowed() {
    let multiplier: usize = 4;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let multiplier_ref = &multiplier;

    let a = thread
        .line(move |w: poll::LineWakers| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w.output),
        })
        .input::<poll::Sync>();

    let mut input = Writer::new(&a).unwrap();

    let mut b = thread
        .line(move |w: poll::LineWakers| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w.output),
        })
        .parent(a)
        .output::<poll::Sync>();

    let output = Receiver::new();
    make_push(&mut b, &output).unwrap();

    thread.add(b);

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        // 1 → ×4 (a) → 4 → ×4 (b) → 16
        input.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(16));

        input.push(Message::Data(3)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(48));

        input.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}

/// Add a `Box<dyn Sink + 'params>` directly onto a Sync poll line
/// node's output `Vec`. The sink holds `&config`; without `'params` on
/// `Edge::Output` and the matching `Add` impl this would not compile.
#[test]
fn poll_borrowed_output_sink() {
    let multiplier: usize = 2;
    let config: usize = 99;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let multiplier_ref = &multiplier;
    let mut node = thread
        .line(move |w: poll::LineWakers| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w.output),
        })
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let mut input = Writer::new(&node).unwrap();
    let output = Receiver::new();
    let forward = Get::get(&output).unwrap();
    let sink = BorrowingSink {
        _config: &config,
        forward,
    };
    Add::add(&mut node, box_borrowing_sink(sink)).unwrap();

    thread.add(node);

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        input.push(Message::Data(6)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(12));

        input.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = (multiplier, config);
}

/// `make_push(borrowed_parent_node, &poll_node)` — exercises the poll
/// node's `Get<dyn Sink + 'params>` impl from the *child* side. If
/// the impl is `'static`-defaulted, `'params` collapses and the borrowed
/// parent fails to compile.
#[test]
fn poll_node_input_from_borrowed_parent() {
    let mult: usize = 2;
    let mut work_line = make_line(BorrowingLine::new(&mult));

    let mut poll_thread = poll::Thread::<'_, LifetimePollThread>::new();
    let mult_ref = &mult;
    let mut poll_node = poll_thread
        .line(move |w: poll::LineWakers| PollBorrowingLine {
            multiplier: mult_ref,
            output: OutputQueue::new(w.output),
        })
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let mut input = Writer::new(work_line.as_ref()).unwrap();
    make_push(work_line.as_mut(), &poll_node).unwrap();

    let output = Receiver::new();
    make_push(&mut poll_node, &output).unwrap();

    poll_thread.add(poll_node);
    let mut work_thread = ThreadStream::<'_, LifetimePollThread>::new();
    make_work::<Unary, _>(work_line, &mut work_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(work_thread);
    bundle.add(poll_thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        // 3 → work_line ×2 → 6 → poll_node ×2 → 12
        input.push(Message::Data(3)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(12));

        input.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = mult;
}
