//! Lifetime cascade through the [`Writer`](crate::work::Writer) wrapper.
//!
//! `work::Writer` and `WriterBuffer`'s `Get` impls carry `'params` so
//! they bridge to borrowed-routine graph nodes. Without `'params`,
//! `Writer::of(borrowed_node)` and `Writer::new(&buffer)` would force
//! the routine's `'params` to `'static`.

use super::mock::{BorrowingLine, LifetimeThread};
use crate::connect::sync::Receiver;
use crate::marker::Unary;
use crate::node::line::work::bridge::from_pull;
use crate::pull::Connect as PullConnect;
use crate::thread::{ThreadBundle, ThreadStream};
use crate::work::{Writer, make_line};
use crate::writer::pull::WriterBuffer;
use crate::{Closeable, Message, Pushable, make_push, make_work};

/// [`Writer::of`] on a borrowed work-line node.
#[test]
fn work_writer_of_borrowed_line() {
    let multiplier: usize = 3;
    let mut line = make_line(BorrowingLine::new(&multiplier));

    let mut writer = Writer::new(line.as_ref()).unwrap();
    let output = Receiver::new();
    make_push(line.as_mut(), &output).unwrap();

    let mut thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(line, &mut thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        writer.push(Message::Data(4)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(12));

        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}

/// [`Writer::new`] on a [`WriterBuffer`] feeding a borrowed pull-line.
/// Exercises `WriterBuffer`'s `Get<dyn Sink + 'params>` impl.
#[test]
fn work_writer_new_from_buffer_with_borrowed_pull() {
    let multiplier: usize = 2;
    let buffer = WriterBuffer::new();
    let mut writer = Writer::new(&buffer).unwrap();

    let pull_line = PullConnect::pull(buffer, BorrowingLine::new(&multiplier));
    let mut bridged = from_pull(pull_line, BorrowingLine::new(&multiplier));
    let output = Receiver::new();
    make_push(bridged.as_mut(), &output).unwrap();

    let mut thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(bridged, &mut thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        // 1 → ×2 (pull) → 2 → ×2 (work) → 4
        writer.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(4));

        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}
