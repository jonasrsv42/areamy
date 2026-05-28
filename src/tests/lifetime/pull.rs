//! Borrowed routines on the `Pullable` connection trait, bridged into
//! the work graph.

use super::mock::{BorrowingLine, LifetimeThread};
use crate::connect::sync::Receiver;
use crate::graph::Get;
use crate::marker::Unary;
use crate::node::line::work::bridge::from_pull;
use crate::pull::Connect as PullConnect;
use crate::source::pull::SourceBuffer;
use crate::thread::{ThreadBundle, ThreadStream};
use crate::{Closeable, Message, Trackable, make_push, make_work};

// ============================================================
// line × Pullable (pull → work bridge, through ThreadBundle)
// ============================================================

#[test]
fn line_pull_borrowed() {
    let multiplier: usize = 4;

    // Pull chain: SourceBuffer -> BorrowingLine (pull) -> bridged into work-line
    let buffer: SourceBuffer<usize, Trackable<&'static str>, LifetimeThread> = SourceBuffer::new();
    let mut source: Box<
        dyn Closeable<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
    > = Get::get(&buffer).unwrap();
    let pull_line = PullConnect::<usize, Trackable<&'static str>>::pull(
        buffer,
        BorrowingLine::new(&multiplier),
    );

    // Bridge pull-segment into work-graph (so we can run it on a ThreadStream).
    let mut bridged = from_pull(pull_line, BorrowingLine::new(&multiplier));
    let output = Receiver::<usize, Trackable<&'static str>>::new();
    make_push(bridged.as_mut(), &output).unwrap();

    let mut sync_thread = ThreadStream::<'_, LifetimeThread>::new();
    make_work::<Unary, LifetimeThread>(bridged, &mut sync_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        // Two ×4 stages = ×16
        source.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(16));

        source.close().unwrap();
        let joins = handle.join().errors();
        assert!(joins.is_empty());
    });

    let _ = multiplier;
}
