//! Sibling-thread teardown tests.
//!
//! Each test pins a different cross-thread edge type so we can see
//! exactly which channel still lacks close-on-drop.

use crate::error::{Error, ErrorKind};
use crate::node::line::poll::routine::tests::MockLine;
use crate::poll;
use crate::poll::thread::Thread;
use crate::sync::Receiver;
use crate::thread::Join;
use crate::work::{self, Reader, Writer};
use crate::{
    At, BifurcationRoutine, Flush, LineIo, LineRoutine, Next, Push, ThreadBundle, ThreadStream,
    Trackable, bifurcation, fatal,
};
use crate::{Closeable, Message, Pushable};
use std::collections::VecDeque;

crate::thread_id!(MiddleThread, PollThread);
crate::thread_id!(ProducerThread, Consumer1, Consumer2);

struct FailingMiddle;

impl crate::Send<usize> for FailingMiddle {
    fn send(&mut self, _message: usize) -> Result<(), Error> {
        Ok(())
    }
}

impl Next<usize> for FailingMiddle {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        // Line work loop calls next() before reading input, so erroring
        // here kills the thread on its first poll — no data needed.
        Err(fatal!("intentional middle-node failure"))
    }
}

impl Flush for FailingMiddle {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl LineRoutine<usize, usize> for FailingMiddle {}

struct PassThrough {
    out: VecDeque<usize>,
}

impl PassThrough {
    fn new() -> Self {
        Self {
            out: VecDeque::new(),
        }
    }
}

impl crate::Send<usize> for PassThrough {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        self.out.push_back(m);
        Ok(())
    }
}

impl Next<usize> for PassThrough {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.out.pop_front())
    }
}

impl Flush for PassThrough {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl LineRoutine<usize, usize> for PassThrough {}

#[test]
fn middle_thread_error_does_not_deadlock_drain() {
    let mut middle = work::Line::of(FailingMiddle);
    let sink_node = work::Line::of(PassThrough::new());

    let writer: Writer<usize> = Writer::new(&middle).unwrap();
    Push::<usize>::connect(&mut middle, &sink_node).unwrap();

    let middle_thread = ThreadStream::<MiddleThread>::of(middle);

    let mut bundle = ThreadBundle::new();
    bundle.add(middle_thread);

    let reader: Reader<usize> = Reader::new(sink_node).unwrap();
    let mut io = LineIo::new(writer, reader);

    std::thread::scope(|s| {
        let bundle_handle = bundle.start(s);

        // Drain in a scoped helper thread so the main thread can join the bundle.
        let drain_handle = s.spawn(move || {
            loop {
                match io.read() {
                    Ok(_) => continue,
                    Err(e) if matches!(e.kind, ErrorKind::Closed) => break Ok::<(), Error>(()),
                    Err(other) => break Err(other),
                }
            }
        });

        let joins = bundle_handle.join();
        let drain_result = drain_handle.join().expect("drain helper panicked");

        // Middle errored, so we expect Join::Error there; the
        // property under test is that the bundle returned at all.
        assert_eq!(joins.len(), 1);
        assert!(
            matches!(&joins[0], Join::Error(_)),
            "expected Join::Error for the failing middle thread, got {:?}",
            joins[0]
        );
        assert!(
            drain_result.is_ok(),
            "drain failed: {:?}",
            drain_result.err()
        );
    });
}

/// Cross-thread sync → poll teardown.
///
/// A poll thread holds one node with a `Sync` input edge. The
/// producer side (held on the main thread as a `Box<dyn Sink>`)
/// is dropped *without* calling `close()`, simulating a sync
/// producer that dies mid-graph.
///
/// The dropped sender's refcounted close-on-drop fires the poll
/// node's `Waker`; the poll node's input phase observes `Closed` and
/// the poll thread terminates cleanly. Without close-on-drop on the
/// sync→poll bridge the poll thread's `consumer.next()` would block
/// forever.
#[test]
fn poll_thread_does_not_deadlock_when_sync_input_drops() {
    let mut thread = Thread::<'_, PollThread>::new();
    let node = thread
        .line(MockLine::new)
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let input = Writer::new(&node).unwrap();

    thread.add(node);
    std::thread::scope(|s| {
        let handle = thread.start(s);

        // Producer dies without calling close().
        drop(input);

        let join = handle.join();
        assert!(
            matches!(join, Join::Ok),
            "expected Join::Ok, got {:?}",
            join
        );
    });
}

// ---- shared routines for the topology tests below ----

/// Bifurcation tee: each input value is duplicated to both outputs.
/// Lets us prove that a single producer thread can fan data out to
/// two sibling consumer threads through bifurcation.
struct Tee {
    left: VecDeque<usize>,
    right: VecDeque<usize>,
}
impl Tee {
    fn new() -> Self {
        Self {
            left: VecDeque::new(),
            right: VecDeque::new(),
        }
    }
}
impl crate::Send<usize> for Tee {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        self.left.push_back(m);
        self.right.push_back(m);
        Ok(())
    }
}
impl Next<usize, bifurcation::Left> for Tee {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.left.pop_front())
    }
}
impl Next<usize, bifurcation::Right> for Tee {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.right.pop_front())
    }
}
impl Flush for Tee {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
impl BifurcationRoutine<usize, usize, usize> for Tee {}

/// Fan-out: one producer thread runs a bifurcation that duplicates
/// each value to two consumer threads, each pushing to its own sync
/// receiver on the main thread. Closing the writer cascades through
/// the bifurcation and both consumer threads exit cleanly.
#[test]
fn fan_out_bifurcation_into_two_consumer_threads() {
    let mut tee = work::Bifurcation::of(Tee::new());
    let mut consumer_a = work::Line::of(PassThrough::new());
    let mut consumer_b = work::Line::of(PassThrough::new());

    let mut writer: Writer<usize> = Writer::new(&tee).unwrap();

    Push::<usize>::connect(&mut tee.at::<bifurcation::Left>(), &consumer_a).unwrap();
    Push::<usize>::connect(&mut tee.at::<bifurcation::Right>(), &consumer_b).unwrap();

    let output_a: Receiver<usize, Trackable<&'static str>> = Receiver::new();
    let output_b: Receiver<usize, Trackable<&'static str>> = Receiver::new();
    Push::<usize>::connect(&mut consumer_a, &output_a).unwrap();
    Push::<usize>::connect(&mut consumer_b, &output_b).unwrap();

    let producer_thread = ThreadStream::<ProducerThread>::of(tee);
    let consumer_a_thread = ThreadStream::<Consumer1>::of(consumer_a);
    let consumer_b_thread = ThreadStream::<Consumer2>::of(consumer_b);

    let mut bundle = ThreadBundle::new();
    bundle
        .add(producer_thread)
        .add(consumer_a_thread)
        .add(consumer_b_thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        for v in 0..4 {
            writer.push(Message::Data(v)).unwrap();
        }
        writer.close().unwrap();

        let drain = |rx: Receiver<usize, Trackable<&'static str>>| -> Vec<usize> {
            let mut got = Vec::new();
            loop {
                match rx.read_front() {
                    Ok(Message::Data(v)) => got.push(v),
                    Ok(_) => continue,
                    Err(e) if matches!(e.kind, ErrorKind::Closed) => break,
                    Err(other) => panic!("unexpected drain error: {:?}", other),
                }
            }
            got
        };
        let a = drain(output_a);
        let b = drain(output_b);
        let joins = handle.join();

        assert_eq!(a, vec![0, 1, 2, 3]);
        assert_eq!(b, vec![0, 1, 2, 3]);
        assert_eq!(joins.len(), 3);
        for j in joins.iter() {
            assert!(matches!(j, Join::Ok), "expected Join::Ok, got {:?}", j);
        }
    });
}
