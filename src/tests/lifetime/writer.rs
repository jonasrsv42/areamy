//! Lifetime cascade through the [`Writer`](crate::work::Writer) wrapper.
//!
//! `work::Writer` and `WriterBuffer`'s `Get` impls carry `'params` so
//! they bridge to borrowed-routine graph nodes. Without `'params`,
//! `borrowed_node.writer()` and `buffer.writer()` would force
//! the routine's `'params` to `'static`.

use super::mock::{BorrowingLine, LifetimeThread};
use crate::edge::sync::Receiver;
use crate::thread::ThreadBundle;
use crate::work;
use crate::writer::pull::WriterBuffer;
use crate::{Closeable, Message, Pullable, Pushable, Pushes, Streams, Via, Writes};

/// [`Writes::writer`] on a borrowed work-line node.
#[test]
fn work_writer_of_borrowed_line() {
    let multiplier: usize = 3;
    let mut line = work::Line::of(BorrowingLine::new(&multiplier));

    let mut writer = line.writer().unwrap();
    let output = Receiver::new();
    line.pushes(&output).unwrap();

    let thread = line.stream::<LifetimeThread>();

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

/// [`Writes::writer`] on a [`WriterBuffer`] feeding a borrowed pull-line.
/// Exercises `WriterBuffer`'s `Get<dyn Sink + 'params>` impl.
#[test]
fn work_writer_from_buffer_with_borrowed_pull() {
    let multiplier: usize = 2;
    let buffer = WriterBuffer::new();
    let mut writer = buffer.writer().unwrap();

    let pull_line = buffer.then(BorrowingLine::new(&multiplier));
    let mut bridged = work::Pulled::of(pull_line)
        .via(work::Line::of(BorrowingLine::new(&multiplier)))
        .unwrap();
    let output = Receiver::new();
    bridged.pushes(&output).unwrap();

    let thread = bridged.stream::<LifetimeThread>();

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
