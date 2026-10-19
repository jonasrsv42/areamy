//! The device assistant: bumbum firmware's graph, minus the real work.
//!
//! ```text
//! mic → into → skip → brain → smoother → flat_map → tick → speaker edge
//!       └────────────── one poll thread ──────────────┘
//! ```
//!
//! The sync stages forward frames unchanged. The brain collects a request, waits on the network,
//! then replies in one burst, sized for a worst-case replay; the smoother collects replies into
//! chunks, with a deadline on each receive. The bench thread is the mic (see `harness`).
//!
//! - `unpaced`: the speaker drains as fast as it can, faster than the graph, so a bounded speaker
//!   edge swings between empty and full instead of staying full. Wall time is the graph's
//!   throughput plus the wakes of those swings; noisy when bounded, as the swings vary.
//! - `slow_speaker`: the speaker is far slower than the graph, so the edge stays full and the
//!   graph parks after nearly every push, woken per freed slot. Wall time is the speaker's; the
//!   metric is the graph thread's CPU, which also holds the wakes from the mic's pushes.
//!
//! `sleep(ZERO)` resolves without registering a timer, so only the smoother's deadlines touch the
//! timer heap.

use crate::harness::{self, Bench, Graph, Metric, Signal};
use crate::support::{AudioFrame, SyncHop};
use areamy::error::Error;
use areamy::graph::Get;
use areamy::poll::future::line::FutureRoutine;
use areamy::poll::future::{Input, input, output};
use areamy::sync::Receiver;
use areamy::{Pushes, ThreadBundle, ThreadId, Via, poll};
use criterion::Criterion;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

areamy::thread_id!(GraphThread);

/// Frames the brain takes in before it replies, and the frames in each reply burst: a worst
/// case, such as replaying a long answer as many small chunks. One batch is one request and its
/// reply.
const BRAIN_BURST: usize = harness::BATCH;
/// Frames the smoother collects before releasing them.
const CHUNK: usize = 5;
/// Frames the speaker's output edge holds.
const SPEAKER_BOUND: NonZeroUsize = NonZeroUsize::new(8).unwrap();
/// A speaker far slower than the graph (about 0.7 µs per frame unpaced): closer, the graph
/// wakes late enough that slots have piled up, and its wakes per frame follow the host's latency.
const SLOW_SPEAKER: Duration = Duration::from_micros(6);
/// The network's answer time; none when unpaced.
const NETWORK: Duration = Duration::ZERO;
/// The smoother releases a partial chunk after this long without a frame. Firmware's is 1 s;
/// this is long enough that preemption doesn't trip it, and short enough to fail fast.
const STREAMING_TIMEOUT: Duration = Duration::from_millis(100);

const _: () = assert!(
    harness::BATCH.is_multiple_of(BRAIN_BURST) && harness::BATCH.is_multiple_of(CHUNK),
    "a batch must drain"
);

/// Partial chunks the smoother released on a timeout. Frames arrive in whole chunks, so any is a
/// stall inside the measurement.
static PARTIALS: AtomicUsize = AtomicUsize::new(0);

/// Collects `BRAIN_BURST` frames as one request, waits for the network, then replies with a burst.
async fn brain(
    input: input::Receiver<AudioFrame>,
    output: output::Sender<AudioFrame>,
) -> Result<(), Error> {
    let mut request = Vec::with_capacity(BRAIN_BURST);
    loop {
        while request.len() < BRAIN_BURST {
            match input.recv().await? {
                Input::Data(frame) => request.push(frame),
                Input::Flush => return reply(&output, &mut request).await,
            }
        }
        poll::sleep(NETWORK).await?;
        reply(&output, &mut request).await?;
    }
}

/// Collects frames into chunks of `CHUNK`; a quiet stream releases what it holds.
async fn smoother(
    input: input::Receiver<AudioFrame>,
    output: output::Sender<AudioFrame>,
) -> Result<(), Error> {
    let mut chunk = Vec::with_capacity(CHUNK);
    loop {
        match input.recv_with_timeout(STREAMING_TIMEOUT).await? {
            Some(Input::Data(frame)) => {
                chunk.push(frame);
                if chunk.len() == CHUNK {
                    reply(&output, &mut chunk).await?;
                }
            }
            Some(Input::Flush) => return reply(&output, &mut chunk).await,
            None => {
                if !chunk.is_empty() {
                    PARTIALS.fetch_add(1, Ordering::Relaxed);
                }
                reply(&output, &mut chunk).await?;
            }
        }
    }
}

/// Send everything held, in order.
async fn reply(
    output: &output::Sender<AudioFrame>,
    held: &mut Vec<AudioFrame>,
) -> Result<(), Error> {
    for frame in held.drain(..) {
        output.send(frame).await?;
    }
    Ok(())
}

fn graph<'p>(bound: Option<NonZeroUsize>) -> Graph<'p> {
    let mut graph = poll::Thread::<GraphThread>::new();
    let into = graph.line(SyncHop::new).input();
    let mic: harness::Input = Get::get(&into).unwrap();
    let mut tick = into
        .via(graph.line(SyncHop::new))
        .unwrap()
        .via(graph.line(FutureRoutine::factory(brain)))
        .unwrap()
        .via(graph.line(FutureRoutine::factory(smoother)))
        .unwrap()
        .via(graph.line(SyncHop::new))
        .unwrap()
        .via(graph.line(SyncHop::new))
        .unwrap()
        .output();
    let speaker = match bound {
        Some(bound) => Receiver::<AudioFrame, Signal>::bounded(bound),
        None => Receiver::new(),
    };
    tick.pushes(&speaker).unwrap();
    graph.add(tick);

    let mut threads = ThreadBundle::new();
    threads.add(graph);
    Graph::new(mic, threads, speaker).check(no_partials)
}

fn no_partials() {
    let partials = PARTIALS.swap(0, Ordering::Relaxed);
    assert_eq!(partials, 0, "the smoother timed out mid-chunk");
}

pub fn assistant(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/realistic/assistant");
    let bounded = Some(SPEAKER_BOUND);

    // Wall time only: its CPU varies with the swings as much as its wall time does.
    Bench::new(&mut group, &format!("unpaced/bound={SPEAKER_BOUND}")).run(|| graph(bounded));
    Bench::new(&mut group, "unpaced/bound=none").run(|| graph(None));
    let cost = SLOW_SPEAKER.as_micros();
    Bench::new(
        &mut group,
        &format!("slow_speaker/cost={cost}us/bound={SPEAKER_BOUND}"),
    )
    .output_cost(SLOW_SPEAKER)
    .metric(Metric::Cpu(GraphThread::name()))
    .run(|| graph(bounded));
    group.finish();
}
