//! Borrowed routines on the `Pullable` connection trait, bridged into
//! the work graph.

use super::mock::{BorrowingLine, LifetimeThread};
use crate::connect::sync::Receiver;
use crate::marker::Unary;
use crate::node::line::work::bridge::from_pull;
use crate::pull::Connect as PullConnect;
use crate::thread::{ThreadBundle, ThreadStream};
use crate::work::Writer;
use crate::writer::pull::WriterBuffer;
use crate::{Closeable, Message, Pushable, make_push, make_work};

#[test]
fn line_pull_borrowed() {
    let multiplier: usize = 4;

    // Pull chain: WriterBuffer -> BorrowingLine (pull) -> bridged into work-line
    let buffer = WriterBuffer::new();
    let mut writer = Writer::new(&buffer).unwrap();
    let pull_line = PullConnect::pull(buffer, BorrowingLine::new(&multiplier));

    // Bridge pull-segment into work-graph (so we can run it on a ThreadStream).
    let mut bridged = from_pull(pull_line, BorrowingLine::new(&multiplier));
    let output = Receiver::new();
    make_push(bridged.as_mut(), &output).unwrap();

    let mut thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, _>(bridged, &mut thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        // Two ×4 stages = ×16
        writer.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(16));

        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}
