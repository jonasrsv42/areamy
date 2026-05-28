//! Lifetime cascade through the [`Source`](crate::work::Source) wrapper.
//!
//! `work::Source` and `SourceBuffer`'s `Get` impls carry `'params` so
//! they bridge to borrowed-routine graph nodes. Without `'params`,
//! `Source::of(borrowed_node)` and `Source::new(&buffer)` would force
//! the routine's `'params` to `'static`.

use super::mock::{BorrowingLine, LifetimeThread};
use crate::connect::sync::Receiver;
use crate::marker::Unary;
use crate::node::line::work::bridge::from_pull;
use crate::pull::Connect as PullConnect;
use crate::source::pull::SourceBuffer;
use crate::thread::{ThreadBundle, ThreadStream};
use crate::work::{Source, make_line};
use crate::{Closeable, Message, Pushable, make_push, make_work};

/// [`Source::of`] on a borrowed work-line node.
#[test]
fn work_source_of_borrowed_line() {
    let multiplier: usize = 3;
    let mut line = make_line(BorrowingLine::new(&multiplier));

    let mut source = Source::new(line.as_ref()).unwrap();
    let output = Receiver::new();
    make_push(line.as_mut(), &output).unwrap();

    let mut thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(line, &mut thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        source.push(Message::Data(4)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(12));

        source.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}

/// [`Source::new`] on a [`SourceBuffer`] feeding a borrowed pull-line.
/// Exercises `SourceBuffer`'s `Get<dyn GraphPushSource + 'params>` impl.
#[test]
fn work_source_new_from_buffer_with_borrowed_pull() {
    let multiplier: usize = 2;
    let buffer = SourceBuffer::new();
    let mut source = Source::new(&buffer).unwrap();

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
        source.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(4));

        source.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}
