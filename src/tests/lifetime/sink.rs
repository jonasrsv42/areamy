//! Lifetime cascade through the [`Sink`](crate::work::Sink) wrappers.
//!
//! `work::Sink`, `tee::Sink` and `pull::Sink` each carry `'params` so
//! they own / borrow a non-`'static` workable or pullable. Without
//! `'params`, none of them could wrap a borrowed-routine graph node.

use super::mock::BorrowingLine;
use crate::marker::Unary;
use crate::pull::{self, Connect as PullConnect};
use crate::sink::work::tee;
use crate::source::pull::SourceBuffer;
use crate::work::{self, Source, make_line};
use crate::{LineReader, Message, Pullable, Pushable};

/// [`work::Sink`] owning a borrowed work-line. Drives the line on
/// the main thread (no `ThreadBundle` — `Sink::read` is synchronous).
#[test]
fn work_sink_owns_borrowed_line() {
    let multiplier: usize = 3;
    let line = make_line(BorrowingLine::new(&multiplier));

    let source = Source::<usize>::of(line.as_ref()).unwrap();
    let sink = work::Sink::new(line).unwrap();
    let mut reader = LineReader::new(source, sink);

    reader.push(Message::Data(4)).unwrap();
    assert_eq!(reader.read().unwrap(), Message::Data(12));

    let _ = multiplier;
}

/// [`tee::Sink`] attached to a borrowed work-line as an output Receiver.
#[test]
fn tee_sink_attached_to_borrowed_line() {
    let multiplier: usize = 5;
    let mut line = make_line(BorrowingLine::new(&multiplier));

    let mut tee_sink = tee::Sink::new::<Unary>(line.as_mut()).unwrap();
    let source = Source::new(line.as_ref()).unwrap();

    let mut reader = LineReader::new(source, work::Sink::new(line).unwrap());
    reader.push(Message::Data(2)).unwrap();

    // Reader drives line.work(); both the work::Sink buffer and the tee
    // Receiver see the same output.
    assert_eq!(reader.read().unwrap(), Message::Data(10));
    assert_eq!(tee_sink.read().unwrap(), Message::Data(10));

    let _ = multiplier;
}

/// [`pull::Sink`] wrapping a borrowed pull-line.
#[test]
fn pull_sink_wraps_borrowed_pull_line() {
    let multiplier: usize = 4;
    let buffer = SourceBuffer::new();
    let mut source = Source::new(&buffer).unwrap();

    let pull_line = PullConnect::pull(buffer, BorrowingLine::new(&multiplier));
    let mut sink = pull::Sink::new(pull_line);

    source.push(Message::Data(2)).unwrap();
    assert_eq!(sink.pull().unwrap(), Message::Data(8));

    let _ = multiplier;
}
