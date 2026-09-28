//! Backpressure in a work graph: a slow consumer blocks its producer.

use crate::Push;
use crate::error::ErrorKind;
use crate::graph::{Closeable, Pushable};
use crate::message::Message;
use crate::node::line::routine::tests::MockWaitLine;
use crate::thread::Join;
use crate::work::{Line, Reader, ThreadStream, Writer};
use std::num::NonZeroUsize;
use std::thread;
use std::time::Duration;

crate::thread_id!(Producer);

const BOUND: NonZeroUsize = NonZeroUsize::new(4).unwrap();

#[test]
fn slow_consumer_blocks_the_producer_thread() {
    const BURST: usize = 50;
    // Producer thread: source → Push side path → bounded sink, read slowly on this thread.
    let mut source = Line::of(MockWaitLine::new(1));
    let mut writer = Writer::new(&source).unwrap();
    let sink = Line::builder().bounded(BOUND).build(MockWaitLine::new(1));
    Push::connect(&mut source, &sink).unwrap();
    let mut reader = Reader::new(sink).unwrap();

    // The whole burst is queued up front; closing lets the producer thread end once it's out.
    for value in 0..BURST {
        writer.push(Message::Data(value)).unwrap();
    }
    writer.close().unwrap();

    thread::scope(|scope| {
        let producer = ThreadStream::<Producer>::of(source).start(scope);
        for value in 0..BURST {
            // Slower than the producer: the sink fills, the source's push is refused `Full`,
            // then `Stuck`, and the producer thread blocks. Run with `--nocapture` to see the
            // "blocked" / "unblocked" pairs.
            thread::sleep(Duration::from_millis(1));
            assert_eq!(reader.read().unwrap(), Message::Data(value));
        }
        // The input's close follows the burst through the blocked side path.
        assert!(matches!(reader.read().unwrap_err().kind, ErrorKind::Closed));
        assert!(matches!(producer.join(), Join::Ok));
    });
}
