//! Sibling-thread teardown tests.
//!
//! Each test pins a different cross-thread edge type so we can see
//! exactly which channel still lacks close-on-drop.

use crate::Closeable;
use crate::Message;
use crate::Pushable;
use crate::error::{Error, ErrorKind};
use crate::graph::Get;
use crate::node::Name;
use crate::sync::Receiver;
use crate::thread::Join;
use crate::work::{Connect, Sink, Source, make_bifurcation, make_line};
use crate::{
    BifurcationRoutine, Flush, LineReader, LineRoutine, Next, ThreadBundle, ThreadId, ThreadStream,
    Trackable, bifurcation, fatal, make_work,
};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::time::Duration;

#[derive(Debug, Clone)]
struct MiddleThread;
impl ThreadId for MiddleThread {}

#[derive(Debug, Clone)]
struct PollThread;
impl ThreadId for PollThread {}

#[derive(Debug, Clone)]
struct ProducerThread;
impl ThreadId for ProducerThread {}
#[derive(Debug, Clone)]
struct Consumer1;
impl ThreadId for Consumer1 {}
#[derive(Debug, Clone)]
struct Consumer2;
impl ThreadId for Consumer2 {}

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

impl Name for FailingMiddle {}
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

impl Name for PassThrough {}
impl LineRoutine<usize, usize> for PassThrough {}

#[test]
fn middle_thread_error_does_not_deadlock_drain() {
    let mut middle = make_line(FailingMiddle);
    let sink_node = make_line(PassThrough::new());

    let source: Source<usize> = Source::new(&middle).unwrap();
    Connect::<usize>::push(&mut middle, &sink_node).unwrap();

    let mut middle_thread = ThreadStream::<MiddleThread>::new();
    make_work(middle, &mut middle_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle.add(middle_thread);

    let sink: Sink<usize> = Sink::new(sink_node).unwrap();
    let mut reader = LineReader::new(source, sink);

    let bundle_handle = bundle.start();

    let (done_tx, done_rx) = mpsc::channel();

    std::thread::spawn(move || {
        let join_result = bundle_handle.join();

        let drain_result = loop {
            match reader.read() {
                Ok(_) => continue,
                Err(e) if matches!(e.kind, ErrorKind::Closed) => break Ok::<(), Error>(()),
                Err(other) => break Err(other),
            }
        };

        let _ = done_tx.send((join_result, drain_result));
    });

    match done_rx.recv_timeout(Duration::from_secs(5)) {
        Ok((joins, drain_result)) => {
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
        }
        Err(_) => panic!(
            "Deadlock: drain + bundle.join did not complete within 5s — \
             sibling-stop did not fire."
        ),
    }
}

/// Cross-thread sync → poll teardown.
///
/// A poll thread holds one node with a `Sync` input edge. The
/// producer side (held on the main thread as a `Box<dyn Closeable>`)
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
    use crate::node::line::poll::routine::tests::MockLine;
    use crate::thread::poll::stream::Thread;

    let mut thread = Thread::<PollThread>::new();
    let node = thread
        .line(|w| MockLine::new(w))
        .input::<crate::poll::Sync>()
        .output::<crate::poll::Sync>();

    let input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
        Get::get(&node).unwrap();

    thread.add(node);
    let handle = thread.start();

    // Producer dies without calling close().
    drop(input);

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(handle.join());
    });

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(join) => assert!(
            matches!(join, Join::Ok),
            "expected Join::Ok, got {:?}",
            join
        ),
        Err(_) => panic!(
            "Deadlock: poll thread did not exit within 5s after sync \
             producer was dropped — SyncBridge needs close-on-drop."
        ),
    }
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
impl Name for Tee {}
impl BifurcationRoutine<usize, usize, usize> for Tee {}

/// Run a closure on a helper thread and recv-timeout the result so a
/// deadlock fails the test instead of hanging the runner.
fn with_timeout<T: Send + 'static>(
    secs: u64,
    label: &str,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(work());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(v) => v,
        Err(_) => panic!("Deadlock: {} did not complete within {}s", label, secs),
    }
}

/// Fan-out: one producer thread runs a bifurcation that duplicates
/// each value to two consumer threads, each pushing to its own sync
/// receiver on the main thread. Closing the source cascades through
/// the bifurcation and both consumer threads exit cleanly.
#[test]
fn fan_out_bifurcation_into_two_consumer_threads() {
    let mut tee = make_bifurcation(Tee::new());
    let mut consumer_a = make_line(PassThrough::new());
    let mut consumer_b = make_line(PassThrough::new());

    let mut source: Source<usize> = Source::new(&tee).unwrap();

    Connect::<usize>::push::<bifurcation::Left, crate::marker::Unary>(
        tee.as_mut(),
        consumer_a.as_ref(),
    )
    .unwrap();
    Connect::<usize>::push::<bifurcation::Right, crate::marker::Unary>(
        tee.as_mut(),
        consumer_b.as_ref(),
    )
    .unwrap();

    let output_a: Receiver<usize, Trackable<&'static str>> = Receiver::new();
    let output_b: Receiver<usize, Trackable<&'static str>> = Receiver::new();
    Connect::<usize>::push(&mut consumer_a, &output_a).unwrap();
    Connect::<usize>::push(&mut consumer_b, &output_b).unwrap();

    let mut producer_thread = ThreadStream::<ProducerThread>::new();
    let mut consumer_a_thread = ThreadStream::<Consumer1>::new();
    let mut consumer_b_thread = ThreadStream::<Consumer2>::new();
    make_work(tee, &mut producer_thread).unwrap();
    make_work(consumer_a, &mut consumer_a_thread).unwrap();
    make_work(consumer_b, &mut consumer_b_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle
        .add(producer_thread)
        .add(consumer_a_thread)
        .add(consumer_b_thread);
    let handle = bundle.start();

    for v in 0..4 {
        source.push(Message::Data(v)).unwrap();
    }
    source.close().unwrap();

    let result = with_timeout(5, "fan_out drain + join", move || {
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
        (a, b, joins)
    });

    let (a, b, joins) = result;
    assert_eq!(a, vec![0, 1, 2, 3]);
    assert_eq!(b, vec![0, 1, 2, 3]);
    assert_eq!(joins.len(), 3);
    for j in joins.iter() {
        assert!(matches!(j, Join::Ok), "expected Join::Ok, got {:?}", j);
    }
}
