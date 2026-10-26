//! Graphs of up to tens of thousands of ids, for how the scheduler scales with graph size rather
//! than with today's graphs.
//!
//! `ids` counts the poll thread's ids: three per line (input, work, output). Idle lines pad a
//! graph to its size; their inputs stay open and nothing reaches them after their first poll.
//! Frames are `usize`, so nothing allocates per frame and each node's work stays small.
//!
//! - `sparse`: one routine wakes itself on every poll, the rest idle. An iteration is one batch
//!   of one id: the per-wake cost of a mostly idle graph.
//! - `burst`: one frame into each of `ids / 3` parallel lines at once, read back off one edge.
//! - `remote`: one frame at a time from the bench thread into one line of an idle graph. A remote
//!   wake polls only its own id.

use crate::harness::cpu_clock::CpuClock;
use crate::harness::{Metric, Signal};
use crate::support::SyncHop;
use areamy::error::Error;
use areamy::graph::Get;
use areamy::poll::{Context, Thread};
use areamy::sync::Receiver;
use areamy::thread::Join;
use areamy::{
    Accept, Closeable, Flush, Message, Next, Poll, Pushable, Pushes, ThreadId, TrySend, poll, work,
};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput};
use std::hint::{self, black_box};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

areamy::thread_id!(ScaleThread);

/// Graph sizes in ids.
const IDS: [usize; 3] = [64, 1024, 10240];
/// Ids per line: input, work, output.
const LINE: usize = 3;
/// The spinner's polls per published count, so the bench thread's loads don't slow each poll.
const PUBLISH: u64 = 64;
/// Fail instead of hanging when an iteration doesn't finish.
const STALL: Duration = Duration::from_secs(5);

type Writer = work::DynSink<'static, usize, Signal>;

/// An input that closes when dropped: dropping a graph's inputs ends it.
struct Closing(Writer);

impl Drop for Closing {
    fn drop(&mut self) {
        let _ = self.0.close();
    }
}

/// A graph to bench: its poll thread, what an iteration drives, and the idle lines' inputs.
struct Scaled<'p, D> {
    thread: Thread<'p, ScaleThread>,
    drive: D,
    idle: Vec<Closing>,
    /// The idle lines' output: never pushed to.
    sink: Receiver<usize, Signal>,
}

/// `count` one-hop lines into `output`, each from its own open input.
fn lines(
    thread: &mut Thread<'_, ScaleThread>,
    count: usize,
    output: &Receiver<usize, Signal>,
) -> Vec<Closing> {
    (0..count)
        .map(|_| {
            let mut line = thread.line(SyncHop::<usize>::new).input().output();
            let input = Closing(Get::get(&line).unwrap());
            line.pushes(output).unwrap();
            thread.add(line);
            input
        })
        .collect()
}

/// Pads `thread`, holding `active` lines, with idle lines into `sink` up to `ids`.
fn scaled<'p, D>(
    ids: usize,
    mut thread: Thread<'p, ScaleThread>,
    active: usize,
    sink: Receiver<usize, Signal>,
    drive: D,
) -> Scaled<'p, D> {
    let idle = lines(&mut thread, ids / LINE - active, &sink);
    Scaled {
        thread,
        drive,
        idle,
        sink,
    }
}

/// Polls itself forever: each poll wakes it for the next batch. Publishes its count every
/// `PUBLISH` polls.
struct Spinner {
    polls: u64,
    published: Arc<AtomicU64>,
}

impl TrySend<usize> for Spinner {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        Ok(Accept::Accepted)
    }
}

impl Next<usize> for Spinner {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl Flush for Spinner {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Poll for Spinner {
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        self.polls += 1;
        if self.polls.is_multiple_of(PUBLISH) {
            self.published.store(self.polls, Ordering::Release);
        }
        cx.wake.local.wake();
        Ok(core::task::Poll::Pending)
    }
}

impl poll::LineRoutine<usize, usize> for Spinner {}

/// The spinner's published count and its input, held open.
struct Spinning {
    published: Arc<AtomicU64>,
    _input: Closing,
}

impl Spinning {
    /// The first count published above `past`.
    fn after(&self, past: u64) -> u64 {
        loop {
            let polls = self.published.load(Ordering::Acquire);
            if polls > past {
                return polls;
            }
            hint::spin_loop();
        }
    }
}

fn spinning<'p>(ids: usize) -> Scaled<'p, Spinning> {
    let mut thread = Thread::new();
    let published = Arc::new(AtomicU64::new(0));
    let counter = published.clone();
    let mut line = thread
        .line(move |_| Spinner {
            polls: 0,
            published: counter,
        })
        .input()
        .output();
    let input = Closing(Get::get(&line).unwrap());
    let sink = Receiver::new();
    line.pushes(&sink).unwrap();
    thread.add(line);
    scaled(
        ids,
        thread,
        1,
        sink,
        Spinning {
            published,
            _input: input,
        },
    )
}

/// One input per line, all read back off `output`.
struct Lines {
    inputs: Vec<Closing>,
    output: Receiver<usize, Signal>,
    /// The poll thread's clock, once it has run.
    clock: Option<CpuClock>,
}

impl Lines {
    /// One frame into each input, then every frame back.
    fn round(&mut self) {
        for (frame, input) in self.inputs.iter_mut().enumerate() {
            input.0.push(Message::Data(frame)).unwrap();
        }
        for _ in 0..self.inputs.len() {
            black_box(self.output.read_front().unwrap());
        }
    }

    /// No frame came back twice. After the clock stops.
    fn settled(&self) {
        assert_eq!(self.output.len().unwrap(), 0, "a frame too many");
    }

    /// The poll thread's CPU clock. A round first proves the thread runs, and so has its name.
    fn clock(&mut self) -> CpuClock {
        if self.clock.is_none() {
            self.round();
            self.clock = Some(CpuClock::of(ScaleThread::name()));
        }
        self.clock.expect("set above")
    }
}

/// `active` lines read back off one edge, then idle ones up to `ids`.
fn active<'p>(ids: usize, active: usize) -> Scaled<'p, Lines> {
    let mut thread = Thread::new();
    let output = Receiver::new();
    let inputs = lines(&mut thread, active, &output);
    scaled(
        ids,
        thread,
        active,
        Receiver::new(),
        Lines {
            inputs,
            output,
            clock: None,
        },
    )
}

/// Exits the process when an armed stretch runs past `STALL`: a lost wake would leave a blocking
/// read, or the join after it, waiting forever.
#[derive(Default)]
struct Watchdog {
    armed: Mutex<Option<Instant>>,
    stopped: AtomicBool,
}

impl Watchdog {
    fn arm(&self) {
        *self.armed.lock().unwrap() = Some(Instant::now());
    }

    fn disarm(&self) {
        *self.armed.lock().unwrap() = None;
    }

    fn watch(&self) {
        while !self.stopped.load(Ordering::Relaxed) {
            thread::sleep(STALL / 50);
            if self
                .armed
                .lock()
                .unwrap()
                .is_some_and(|at| at.elapsed() > STALL)
            {
                eprintln!("scale bench stalled: an iteration ran past {STALL:?}");
                process::exit(101);
            }
        }
    }
}

/// Benches `build`'s graph, built on the first sample so a filtered-out id costs nothing.
/// `iterate` runs a sample's iterations and returns their time.
fn bench<'p, D>(
    group: &mut BenchmarkGroup<WallTime>,
    id: &str,
    build: impl FnOnce() -> Scaled<'p, D>,
    mut iterate: impl FnMut(&mut D, u64) -> Duration,
) {
    let watchdog = Arc::new(Watchdog::default());
    let mut build = Some(build);
    thread::scope(|s| {
        let watching = watchdog.clone();
        s.spawn(move || watching.watch());
        let mut running = None;
        group.bench_function(id, |b| {
            let (drive, ..) = running.get_or_insert_with(|| {
                let Scaled {
                    thread,
                    drive,
                    idle,
                    sink,
                } = build.take().expect("built once")();
                (drive, idle, sink, thread.start(s))
            });
            b.iter_custom(|iters| {
                watchdog.arm();
                let elapsed = iterate(drive, iters);
                watchdog.disarm();
                elapsed
            })
        });
        if let Some((drive, idle, sink, handle)) = running {
            // Closing every input ends the graph.
            watchdog.arm();
            drop((drive, idle));
            assert!(matches!(handle.join(), Join::Ok), "the graph failed");
            drop(sink);
        }
        watchdog.stopped.store(true, Ordering::Relaxed);
    });
}

pub fn sparse(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/scale/sparse");
    group.throughput(Throughput::Elements(1));
    for ids in IDS {
        bench(
            &mut group,
            &format!("ids={ids}/{}", Metric::Wall),
            || spinning(ids),
            |spinning, iters| {
                // From one published count to another, so the clock sees whole polls.
                let from = spinning.after(spinning.published.load(Ordering::Acquire));
                let start = Instant::now();
                let to = spinning.after(from + iters - 1);
                let elapsed = start.elapsed();
                // `to - from` polls ran, up to `PUBLISH` more than asked.
                elapsed.mul_f64(iters as f64 / (to - from) as f64)
            },
        );
    }
    group.finish();
}

pub fn burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/scale/burst");
    for ids in IDS {
        let count = ids / LINE;
        group.throughput(Throughput::Elements(count as u64));
        bench(
            &mut group,
            &format!("ids={ids}/{}", Metric::Wall),
            || active(ids, count),
            |lines, iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    lines.round();
                }
                let elapsed = start.elapsed();
                lines.settled();
                elapsed
            },
        );
    }
    group.finish();
}

pub fn remote(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/scale/remote");
    group.throughput(Throughput::Elements(1));
    for ids in IDS {
        bench(
            &mut group,
            &format!("ids={ids}/{}", Metric::Wall),
            || active(ids, 1),
            |line, iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    line.round();
                }
                let elapsed = start.elapsed();
                line.settled();
                elapsed
            },
        );
        let metric = Metric::Cpu(ScaleThread::name());
        if !CpuClock::SUPPORTED {
            eprintln!("skipping poll/scale/remote/ids={ids}/{metric}: it needs 64-bit Linux");
            continue;
        }
        bench(
            &mut group,
            &format!("ids={ids}/{metric}"),
            || active(ids, 1),
            |line, iters| {
                let clock = line.clock();
                let start = clock.now();
                for _ in 0..iters {
                    line.round();
                }
                let elapsed = clock.now() - start;
                line.settled();
                elapsed
            },
        );
    }
    group.finish();
}
