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
use crate::work::make_line;
use crate::{Closeable, Message, biunion, make_push, make_work};

// ============================================================
// line × Pollable (async, through ThreadBundle)
// ============================================================

#[test]
fn line_poll_borrowed() {
    let multiplier: usize = 5;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let multiplier_ref = &multiplier;
    let mut node = thread
        .line(move |w| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w),
        })
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let mut input: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> =
        Get::get(&node).unwrap();
    let output = Receiver::<usize, &'static str>::new();
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
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = multiplier;
}

// ============================================================
// biunion × Pollable (async, through ThreadBundle)
// ============================================================

#[test]
fn biunion_poll_borrowed() {
    let bias: usize = 7;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let bias_ref = &bias;
    let mut node = thread
        .biunion(move |w| PollBorrowingBiunion {
            bias: bias_ref,
            output: OutputQueue::new(w),
        })
        .input::<biunion::Left, poll::Sync>()
        .input::<biunion::Right, poll::Sync>()
        .output::<poll::Sync>();

    let mut left: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> =
        Get::<_, biunion::Left>::get(&node).unwrap();
    let mut right: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> =
        Get::<_, biunion::Right>::get(&node).unwrap();

    let output = Receiver::<usize, &'static str>::new();
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
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = bias;
}

// ============================================================
// Async parent chain in poll (two poll lines chained via .parent())
// ============================================================

/// Two poll line nodes wired with `.parent()` — node `b` consumes
/// node `a` as an `AsyncParent<'params>` (in-thread, no Mutex). Both
/// routines borrow `&multiplier`. Exercises the `AsyncParent<'params>`
/// trait + the Async input edge path with non-`'static` routines.
#[test]
fn poll_async_parent_chain_borrowed() {
    let multiplier: usize = 4;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let multiplier_ref = &multiplier;

    let a = thread
        .line(move |w| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w),
        })
        .input::<poll::Sync>();

    let mut input: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> =
        Get::get(&a).unwrap();

    let mut b = thread
        .line(move |w| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w),
        })
        .parent(a)
        .output::<poll::Sync>();

    let output = Receiver::<usize, &'static str>::new();
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
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = multiplier;
}

// ============================================================
// Borrowed Sync poll output sink (exercises Edge::Output<'params>)
// ============================================================

/// Adds a `Box<dyn Closeable + 'params>` directly onto a Sync poll line
/// node's output `Vec`. The sink holds `&config`, so its lifetime is
/// genuinely shorter than `'static`; without the `'params` bound on
/// [`crate::connect::poll::edge::Edge::Output`] (and the matching `Add`
/// impl) this would not compile.
#[test]
fn poll_borrowed_output_sink() {
    let multiplier: usize = 2;
    let config: usize = 99;
    let mut thread = poll::Thread::<'_, LifetimePollThread>::new();

    let multiplier_ref = &multiplier;
    let mut node = thread
        .line(move |w| PollBorrowingLine {
            multiplier: multiplier_ref,
            output: OutputQueue::new(w),
        })
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let mut input: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> =
        Get::get(&node).unwrap();

    let output = Receiver::<usize, &'static str>::new();
    let forward: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> =
        Get::get(&output).unwrap();
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
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = (multiplier, config);
}

// ============================================================
// make_push from a borrowed parent INTO a poll node
// ============================================================

/// `make_push(borrowed_parent_node, &poll_node)` — exercises the poll
/// node's `Get<dyn Closeable + 'params>` impl from the *child* side.
/// `make_push` unifies parent's `Add<dyn ... + 'params>` with child's
/// `Get<dyn ... + 'params>`; if the poll node's `Get` is hard-coded to
/// `'static` (via object-lifetime default), the unification forces
/// parent's `'params = 'static`, which fails when parent borrows a
/// non-`'static` config.
#[test]
fn poll_node_input_from_borrowed_parent() {
    let mult: usize = 2;

    // Parent: borrowed work line.
    let mut work_line = make_line(BorrowingLine::new(&mult));

    // Child: poll node, also borrowing the same &mult.
    let mut poll_thread = poll::Thread::<'_, LifetimePollThread>::new();
    let mult_ref = &mult;
    let mut poll_node = poll_thread
        .line(move |w| PollBorrowingLine {
            multiplier: mult_ref,
            output: OutputQueue::new(w),
        })
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    // Source feeding the parent.
    let mut input: Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync> =
        Get::get(work_line.as_ref()).unwrap();

    // The key line — must select Get<dyn Closeable + 'params> on poll_node
    // where 'params matches work_line's borrow lifetime (not 'static).
    make_push(work_line.as_mut(), &poll_node).unwrap();

    // Drain at the poll node's output.
    let output = Receiver::<usize, &'static str>::new();
    make_push(&mut poll_node, &output).unwrap();

    poll_thread.add(poll_node);
    let mut work_thread = ThreadStream::<'_, LifetimePollThread>::new();
    make_work::<Unary, LifetimePollThread>(work_line, &mut work_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(work_thread);
    bundle.add(poll_thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        // 3 → work_line ×2 → 6 → poll_node ×2 → 12
        input.push(Message::Data(3)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(12));

        input.close().unwrap();
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = mult;
}
