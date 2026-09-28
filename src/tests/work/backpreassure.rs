//! Backpressure in a work graph: bounded inputs hold back a bursting producer.

use crate::edge::sync::tests::join_within;
use crate::graph::Pushable;
use crate::message::Message;
use crate::node::line::routine::tests::MockWaitLine;
use crate::work::{self, Line, Reader, Writer};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

const BOUND: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Most messages sent but not yet read: each bounded input, each routine's buffer, each held
/// (suspended) message, and the reader's buffer.
const IN_FLIGHT: usize = 2 * BOUND.get() + 2 + 2 + 1;

#[test]
fn bounded_lines_hold_back_a_bursting_writer() {
    const BURST: usize = 1000;
    // writer (another thread) → source → sink → reader, both lines bounded.
    let source = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    let mut sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    let mut writer = Writer::new(&source).unwrap();
    work::Bidi::connect(source, &mut sink).unwrap();
    let mut reader = Reader::new(sink).unwrap();

    // Tries to push the whole burst at once; a full source input makes `push` wait.
    let sent = Arc::new(AtomicUsize::new(0));
    let producer = {
        let sent = sent.clone();
        thread::spawn(move || {
            for value in 0..BURST {
                writer.push(Message::Data(value)).unwrap();
                sent.fetch_add(1, Ordering::Relaxed);
            }
            writer
        })
    };

    for read in 0..BURST {
        assert_eq!(reader.read().unwrap(), Message::Data(read));
        // The writer only ever runs a few messages ahead of the reader: the rest of the burst
        // waits in its blocked `push`, not in memory.
        let ahead = sent.load(Ordering::Relaxed).saturating_sub(read + 1);
        assert!(ahead <= IN_FLIGHT, "{ahead} messages in flight");
    }
    let _writer = join_within(producer);
}
