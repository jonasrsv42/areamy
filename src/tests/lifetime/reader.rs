//! Lifetime cascade through the [`Reader`](crate::work::Reader) wrappers.
//!
//! `work::Reader`, `tee::Reader` and `pull::Reader` each carry `'params` so
//! they own / borrow a non-`'static` workable or pullable. Without
//! `'params`, none of them could wrap a borrowed-routine graph node.

use super::mock::BorrowingLine;
use crate::pull;
use crate::work::{self, Writer, tee};
use crate::writer::pull::WriterBuffer;
use crate::{LineIo, Message, Pullable, Pushable};

/// [`work::Reader`] owning a borrowed work-line. Drives the line on
/// the main thread (no `ThreadBundle` — `Reader::read` is synchronous).
#[test]
fn work_reader_owns_borrowed_line() {
    let multiplier: usize = 3;
    let line = work::Line::of(BorrowingLine::new(&multiplier));

    let writer = Writer::<usize>::of(&line).unwrap();
    let reader = work::Reader::new(line).unwrap();
    let mut io = LineIo::new(writer, reader);

    io.push(Message::Data(4)).unwrap();
    assert_eq!(io.read().unwrap(), Message::Data(12));

    let _ = multiplier;
}

/// [`tee::Reader`] attached to a borrowed work-line as an output Receiver.
#[test]
fn tee_reader_attached_to_borrowed_line() {
    let multiplier: usize = 5;
    let mut line = work::Line::of(BorrowingLine::new(&multiplier));

    let mut tee_reader = tee::Reader::new(&mut line).unwrap();
    let writer = Writer::new(&line).unwrap();

    let mut reader = LineIo::new(writer, work::Reader::new(line).unwrap());
    reader.push(Message::Data(2)).unwrap();

    // Reader drives line.work(); both the work::Reader buffer and the tee
    // Receiver see the same output.
    assert_eq!(reader.read().unwrap(), Message::Data(10));
    assert_eq!(tee_reader.read().unwrap(), Message::Data(10));

    let _ = multiplier;
}

/// [`pull::Reader`] wrapping a borrowed pull-line.
#[test]
fn pull_reader_wraps_borrowed_pull_line() {
    let multiplier: usize = 4;
    let buffer = WriterBuffer::new();
    let mut writer = Writer::new(&buffer).unwrap();

    let pull_line = buffer.then(BorrowingLine::new(&multiplier));
    let mut reader = pull::Reader::new(pull_line);

    writer.push(Message::Data(2)).unwrap();
    assert_eq!(reader.pull().unwrap(), Message::Data(8));

    let _ = multiplier;
}
