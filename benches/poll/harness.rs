//! Drives a graph with circulating frames, a batch at a time.
//!
//! ```text
//! bench thread → input → graph → output edge → output thread
//!       ↑                                            │
//!       └────────────── frames, once per batch ──────┘
//! ```
//!
//! One batch stays in flight, and each sample starts with an untimed batch, so the clock only
//! sees the pipeline in steady state. Nothing allocates a payload per batch.

#[cfg_attr(
    not(all(target_os = "linux", target_pointer_width = "64")),
    path = "harness/cpu_clock_unsupported.rs"
)]
mod cpu_clock;
mod output;
mod running;

use crate::support::AudioFrame;
use areamy::sync::Receiver;
use areamy::{ThreadBundle, Trackable, work};
use cpu_clock::CpuClock;
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, SamplingMode, Throughput};
use running::Running;
use std::fmt;
use std::thread;
use std::time::Duration;

pub type Signal = Trackable<&'static str>;
pub type Input = Box<dyn work::Sink<DataType = AudioFrame, SignalType = Signal> + Send + Sync>;
pub type Output = Receiver<AudioFrame, Signal>;

/// Frames per measured batch.
pub const BATCH: usize = 1000;

/// A graph to drive: frames go into `input` and come out of `output`.
pub struct Graph<'p> {
    input: Input,
    threads: ThreadBundle<'p>,
    output: Output,
    check: fn(),
}

impl<'p> Graph<'p> {
    pub fn new(input: Input, threads: ThreadBundle<'p>, output: Output) -> Self {
        Graph {
            input,
            threads,
            output,
            check: || {},
        }
    }

    /// Runs after every sample, so a bad run fails before criterion saves it.
    pub fn check(mut self, check: fn()) -> Self {
        self.check = check;
        self
    }
}

/// What a sample reports.
#[derive(Clone, Copy)]
pub enum Metric {
    /// Wall time: the pipeline's throughput, wakes and waits included.
    Wall,
    /// On-CPU time of the named thread, kernel time included, for when wall time is set by
    /// another thread. 64-bit Linux only; skipped elsewhere.
    Cpu(&'static str),
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Metric::Wall => write!(f, "wall"),
            Metric::Cpu(thread) => write!(f, "cpu={thread}"),
        }
    }
}

#[derive(Clone, Copy)]
struct Load {
    batch: usize,
    output_cost: Duration,
    /// Time between the input's pushes; zero pushes as fast as it can.
    pace: Duration,
}

/// One bench id: `Bench::new(&mut group, "unpaced/bound=8").run(|| graph())`. Its id gains the
/// batch and the metric: `unpaced/bound=8/batch=1000/wall`.
pub struct Bench<'g, 'c> {
    group: &'g mut BenchmarkGroup<'c, WallTime>,
    id: String,
    load: Load,
    metric: Metric,
}

impl<'g, 'c> Bench<'g, 'c> {
    pub fn new(group: &'g mut BenchmarkGroup<'c, WallTime>, id: &str) -> Self {
        Bench {
            group,
            id: id.to_string(),
            load: Load {
                batch: BATCH,
                output_cost: Duration::ZERO,
                pace: Duration::ZERO,
            },
            metric: Metric::Wall,
        }
    }

    /// The output thread's busy time per frame: above the graph's, the output edge stays full.
    pub fn output_cost(mut self, cost: Duration) -> Self {
        self.load.output_cost = cost;
        self
    }

    /// Frames per measured batch; a burst must fit in one.
    pub fn batch(mut self, batch: usize) -> Self {
        self.load.batch = batch;
        self
    }

    /// The input's period: one frame per `pace`, for a steady stream instead of a burst.
    pub fn pace(mut self, pace: Duration) -> Self {
        self.load.pace = pace;
        self
    }

    pub fn metric(mut self, metric: Metric) -> Self {
        self.metric = metric;
        self
    }

    /// Benches `build`'s graph. It is built on the first sample, so a filtered-out id costs
    /// nothing.
    pub fn run<'p>(self, build: impl FnOnce() -> Graph<'p>) {
        let Bench {
            group,
            id,
            load,
            metric,
        } = self;
        let id = format!("{id}/batch={}/{metric}", load.batch);
        if matches!(metric, Metric::Cpu(_)) && !CpuClock::SUPPORTED {
            eprintln!("skipping {id}: the cpu metric needs 64-bit Linux");
            return;
        }
        group
            .throughput(Throughput::Elements(load.batch as u64))
            // Linear sampling's short samples would each be mostly the untimed batch.
            .sampling_mode(SamplingMode::Flat);

        let mut build = Some(build);
        thread::scope(|s| {
            // Dropped before the scope joins, also on a panic: the input closes and the graph ends.
            let mut running = None;
            group.bench_function(id, |b| {
                let run = running.get_or_insert_with(|| {
                    let build = build.take().expect("built once");
                    Running::start(s, build(), load, metric)
                });
                b.iter_custom(|batches| {
                    run.batch();
                    let start = run.mark();
                    for _ in 0..batches {
                        run.batch();
                    }
                    let elapsed = start.elapsed();
                    run.check();
                    elapsed
                })
            });
            if let Some(run) = running {
                run.finish();
            }
        });
    }
}
