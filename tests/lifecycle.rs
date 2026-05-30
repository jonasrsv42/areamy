//! End-to-end bundle lifecycle test.
//!
//! Mixes all three graph paradigms in one graph per generation:
//!
//! - **Pull** segment on the main thread: [`SourceBuffer`] →
//!   [`pull::Connect::pull`] line (identity).
//! - **Work** segment on a bundled OS thread: [`from_pull`]-wrapped
//!   line node whose routine ([`FailAfter`]) processes one message
//!   successfully and then errors on the second, so each generation
//!   actually produces real output before the failure path runs.
//! - **Poll** segment on a second bundled OS thread: a poll line
//!   node that doubles its input, with sync↔poll bridge edges on
//!   both sides.
//! - **Sink** end: a [`Receiver`] on the main thread, drained by a
//!   helper thread that accumulates observed values so the main
//!   thread can assert that real data flowed in each generation
//!   before the failure.
//!
//! Demonstrates the recommended orchestrator pattern for restartable
//! graphs:
//!
//! 1. Build graph version `gen`. Register
//!    [`ThreadBundle::on_first_error`] with `gen` captured in the
//!    closure.
//! 2. Start the bundle. Push two items. The work-thread routine
//!    accepts the first (it flows all the way to the sink as a
//!    doubled value), then errors on the second; the callback fires
//!    with a `GraphEvent { generation: gen, kind: Error }`.
//! 3. Orchestrator dequeues. Compares `event.generation` to the
//!    current generation — events from older bundles are discarded;
//!    matching events trigger teardown (close source, join bundle,
//!    join drain thread, inspect collected values) and rebuild with
//!    `gen + 1`.
//! 4. After [`TARGET_GENERATIONS`] cycles, exit.
//!
//! The test injects exactly one stale event per transition to force
//! the dedup path to run, and asserts:
//!
//! - the target number of cycles completed,
//! - exactly `TARGET_GENERATIONS - 1` stale events were observed,
//! - every generation's drain thread saw at least one value flow
//!   through the graph before the error.
//!
//! Cycle / loop topology is exercised separately by
//! `src/tests/cycle_test.rs`; this test stays linear so the
//! lifecycle plumbing is the focus.

use areamy::connect::sync::Receiver;
use areamy::error::Error;
use areamy::node::Name;
use areamy::pull;
use areamy::pull::SourceBuffer;
use areamy::thread::{ThreadBundle, ThreadBundleHandle, ThreadId, ThreadStream};
use areamy::work::{Source, from_pull};
use areamy::{
    Closeable, Flush, LineRoutine, Message, Next, Pushable, Send as RoutineSend, fatal, make_push,
    make_work, poll,
};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, Clone)]
struct WorkThread;
impl ThreadId for WorkThread {}

#[derive(Debug, Clone)]
struct PollThread;
impl ThreadId for PollThread {}

// ---- routines -------------------------------------------------------------

/// Pull-side identity. Carries `usize` through by queueing in `send`
/// and dequeueing in `next` — a no-op `send/next` pair would drop
/// data because the pull line loops until `next_output` returns
/// `Some`.
struct PullIdentity {
    queue: VecDeque<usize>,
}
impl PullIdentity {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }
}
impl RoutineSend<usize> for PullIdentity {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        self.queue.push_back(m);
        Ok(())
    }
}
impl Next<usize> for PullIdentity {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.queue.pop_front())
    }
}
impl Flush for PullIdentity {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
impl Name for PullIdentity {}
impl LineRoutine<usize, usize> for PullIdentity {}

/// Work-side routine: accept `pass_count` messages, error on the
/// next. Lets us push `pass_count` items that flow all the way to
/// the sink and then push one more to trigger the failure path.
struct FailAfter {
    remaining: usize,
    queue: VecDeque<usize>,
}
impl FailAfter {
    fn new(pass_count: usize) -> Self {
        Self {
            remaining: pass_count,
            queue: VecDeque::new(),
        }
    }
}
impl RoutineSend<usize> for FailAfter {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        if self.remaining == 0 {
            return Err(fatal!("FailAfter limit reached"));
        }
        self.remaining -= 1;
        self.queue.push_back(m);
        Ok(())
    }
}
impl Next<usize> for FailAfter {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.queue.pop_front())
    }
}
impl Flush for FailAfter {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
impl Name for FailAfter {}
impl LineRoutine<usize, usize> for FailAfter {}

/// Poll-side doubler. Same shape as `MockLine` but inline so this
/// test is self-contained.
struct PollDouble {
    out: VecDeque<usize>,
}
impl PollDouble {
    fn new(_wakers: areamy::poll::LineWakers) -> Self {
        Self {
            out: VecDeque::new(),
        }
    }
}
impl RoutineSend<usize> for PollDouble {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        self.out.push_back(m * 2);
        Ok(())
    }
}
impl Next<usize> for PollDouble {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.out.pop_front())
    }
}
impl Flush for PollDouble {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
impl areamy::Poll for PollDouble {
    fn poll(
        &mut self,
        _waker: &mut areamy::connect::waker::Waker,
    ) -> Result<core::task::Poll<()>, Error> {
        Ok(core::task::Poll::Pending)
    }
}
impl Name for PollDouble {}
impl areamy::poll::LineRoutine<usize, usize> for PollDouble {}

// ---- orchestrator events --------------------------------------------------

#[derive(Debug, PartialEq)]
enum EventKind {
    Error,
}

#[derive(Debug)]
struct GraphEvent {
    generation: usize,
    kind: EventKind,
}

// ---- per-generation handles ------------------------------------------------

/// Everything the orchestrator hangs onto for one generation.
struct GenerationHandles<'params> {
    bundle: ThreadBundleHandle<'params>,
    source: Source<'params, usize, areamy::Trackable<&'static str>>,
    /// JoinHandle for the helper thread that drains the sink-side
    /// Receiver into `seen`.
    drain: JoinHandle<()>,
    /// Values observed by `drain` before the graph closed.
    seen: Arc<Mutex<Vec<usize>>>,
}

// ---- graph builder --------------------------------------------------------

/// Build and start one generation of the graph.
///
/// `tx`/`generation` is the load-bearing pair: the
/// [`ThreadBundle::on_first_error`] closure captures *this*
/// generation by value, so events reaching the orchestrator carry
/// the generation of the bundle that emitted them — not whatever the
/// orchestrator's current generation happens to be on dequeue.
fn build_graph<'params>(
    scope: &'params std::thread::Scope<'params, 'static>,
    tx: mpsc::Sender<GraphEvent>,
    generation: usize,
) -> GenerationHandles<'params> {
    // Pull segment on the main thread.
    let buffer = SourceBuffer::new();
    let source = Source::new(&buffer).unwrap();
    let pull_line = pull::Connect::pull(buffer, PullIdentity::new());

    // Work segment: wrap the pull tail into a work-line that accepts
    // one message and then errors. Lives on its own OS thread.
    let mut bridged = from_pull(pull_line, FailAfter::new(1));

    // Poll segment: a poll line on a dedicated async thread with a
    // sync→poll bridge on input and a sync edge on output.
    let mut poll_thread = poll::Thread::<'_, PollThread>::new();
    let mut poll_node = poll_thread
        .line(|w| PollDouble::new(w))
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    // Sink-side `Receiver` on the main thread, drained by a helper
    // thread so the orchestrator can verify real data flowed in
    // each generation.
    let sink_recv = Receiver::new();
    make_push(&mut bridged, &poll_node).unwrap();
    make_push(&mut poll_node, &sink_recv).unwrap();

    poll_thread.add(poll_node);

    let mut work_thread = ThreadStream::<'_, WorkThread>::new();
    make_work(bridged, &mut work_thread).unwrap();

    let mut bundle = ThreadBundle::new();
    bundle
        .add(work_thread)
        .add(poll_thread)
        .on_first_error(move |_failure| {
            let _ = tx.send(GraphEvent {
                generation,
                kind: EventKind::Error,
            });
        });

    let handle = bundle.start(scope);

    // Helper thread: drain sink-side Receiver until Closed, append
    // every Data value to `seen`.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_drain = seen.clone();
    let drain = std::thread::spawn(move || {
        loop {
            match sink_recv.read_front() {
                Ok(Message::Data(v)) => seen_drain.lock().unwrap().push(v),
                Ok(_) => {}      // ignore Marker / Flush
                Err(_) => break, // Closed → graph torn down
            }
        }
    });

    GenerationHandles {
        bundle: handle,
        source,
        drain,
        seen,
    }
}

/// Tear down the current generation: close the source so the cascade
/// propagates through the graph, join the bundle (blocks until every
/// thread has exited), then join the drain thread (which exits when
/// it sees Closed on the sink-side Receiver). Returns the values the
/// drain thread observed.
fn teardown(mut gen_handles: GenerationHandles<'_>) -> Vec<usize> {
    let _ = Closeable::close(&mut gen_handles.source);
    let _ = gen_handles.bundle.join();
    let _ = gen_handles.drain.join();
    let observed = gen_handles.seen.lock().unwrap().clone();
    observed
}

const TARGET_GENERATIONS: usize = 3;

#[test]
fn restart_loop_with_generation_dedup_and_real_data_flow() {
    std::thread::scope(|s| {
        let (event_tx, event_rx) = mpsc::channel::<GraphEvent>();

        let mut current_gen: usize = 0;
        let mut completed_cycles = 0;
        let mut stale_events_seen: Vec<usize> = Vec::new();
        let mut per_gen_observed: Vec<Vec<usize>> = Vec::new();

        let mut gen_handles = build_graph(s, event_tx.clone(), current_gen);

        // Push two items: the first flows all the way through (sink sees
        // 1 * 2 = 2); the second triggers the work-line failure path.
        gen_handles.source.push(Message::Data(1)).unwrap();
        gen_handles.source.push(Message::Data(2)).unwrap();

        loop {
            let event = match event_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(e) => e,
                Err(_) => panic!(
                    "timeout: no event at generation {} after {} cycles",
                    current_gen, completed_cycles
                ),
            };

            if event.generation != current_gen {
                stale_events_seen.push(event.generation);
                continue;
            }

            match event.kind {
                EventKind::Error => {
                    let observed = teardown(gen_handles);
                    per_gen_observed.push(observed);
                    completed_cycles += 1;

                    if completed_cycles >= TARGET_GENERATIONS {
                        break;
                    }

                    // Inject one stale event from the gen we just tore
                    // down — the next loop iteration must dequeue it and
                    // discard without rebuilding. Exactly one injection
                    // per transition → `TARGET_GENERATIONS - 1` stales.
                    event_tx
                        .send(GraphEvent {
                            generation: current_gen,
                            kind: EventKind::Error,
                        })
                        .unwrap();

                    current_gen += 1;
                    gen_handles = build_graph(s, event_tx.clone(), current_gen);
                    gen_handles.source.push(Message::Data(1)).unwrap();
                    gen_handles.source.push(Message::Data(2)).unwrap();
                }
            }
        }

        assert_eq!(completed_cycles, TARGET_GENERATIONS);

        // `on_first_error` fires at most once per bundle, so the only
        // stale events are the ones we inject — exactly one per
        // transition.
        assert_eq!(
            stale_events_seen.len(),
            TARGET_GENERATIONS - 1,
            "expected one stale event per transition; got {:?}",
            stale_events_seen
        );
        assert!(
            stale_events_seen.iter().all(|&g| g < current_gen),
            "stale events should all be from older generations, got {:?} with current_gen={}",
            stale_events_seen,
            current_gen
        );

        // Every generation must have produced real data through the
        // graph before its failure path fired — proves we're not just
        // constructing-and-tearing-down without actually doing work.
        assert_eq!(per_gen_observed.len(), TARGET_GENERATIONS);
        for (idx, observed) in per_gen_observed.iter().enumerate() {
            assert!(
                !observed.is_empty(),
                "generation {} produced no output before failure: {:?}",
                idx,
                observed
            );
            // We pushed `1`, FailAfter(1) accepted it, PollDouble made
            // it `2`. The second push triggers the error path and
            // doesn't flow through.
            assert_eq!(
                observed,
                &vec![2usize],
                "generation {} observed unexpected values: {:?}",
                idx,
                observed
            );
        }
    });
}
