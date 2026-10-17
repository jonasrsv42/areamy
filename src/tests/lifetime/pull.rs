//! Borrowed routines on the `Pullable` connection trait, bridged into
//! the work graph.

use super::mock::{BorrowingLine, LifetimeThread};
use crate::edge::sync::Receiver;
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::ThreadBundle;
use crate::work;
use crate::writer::pull::WriterBuffer;
use crate::{Closeable, Message, Pullable, Pushable, Pushes, Streams, Via, Writes};

#[test]
fn line_pull_borrowed() {
    let _deadline = deadline(DEADLINE);
    let multiplier: usize = 4;

    // Pull chain: WriterBuffer -> BorrowingLine (pull) -> bridged into work-line
    let buffer = WriterBuffer::new();
    let mut writer = buffer.writer().unwrap();
    let pull_line = buffer.then(BorrowingLine::new(&multiplier));

    // Bridge pull-segment into work-graph (so we can run it on a ThreadStream).
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

        // Two ×4 stages = ×16
        writer.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(16));

        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });

    let _ = multiplier;
}
