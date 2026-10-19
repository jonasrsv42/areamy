//! One `FutureRoutine` line between two sync edges: the bench thread pushes straight into the
//! poll node's input and reads its output edge.
//!
//! `round_trip` is two OS hand-offs (see `edge/cross_thread/ping_pong`), so runtime costs
//! below a microsecond don't show in it; `burst` shows them.

use areamy::error::Error;
use areamy::graph::Get;
use areamy::poll::future::line::FutureRoutine;
use areamy::poll::future::{Input, input, output};
use areamy::sync::Receiver;
use areamy::{Closeable, Message, Pushable, Pushes, Trackable, poll, work};
use criterion::{Criterion, Throughput};
use std::hint::black_box;
use std::thread;
use std::time::Instant;

areamy::thread_id!(IoThread);

type Signal = Trackable<&'static str>;

pub fn future_line(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/micro/future_line");
    group.throughput(Throughput::Elements(1));

    let mut io_thread = poll::Thread::<IoThread>::new();
    let routine = FutureRoutine::factory(
        |input: input::Receiver<usize>, output: output::Sender<usize>| {
            Box::pin(async move {
                while let Input::Data(value) = input.recv().await? {
                    output.send(value).await?;
                }
                Ok::<_, Error>(())
            })
        },
    );
    let mut node = io_thread.line(routine).input().output();
    let mut writer: Box<dyn work::Sink<DataType = usize, SignalType = Signal> + Send + Sync> =
        Get::get(&node).unwrap();
    let output = Receiver::<usize, Signal>::new();
    node.pushes(&output).unwrap();
    io_thread.add(node);

    thread::scope(|s| {
        let handle = io_thread.start(s);

        group.bench_function("burst", |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                for i in 0..iters {
                    writer.push(Message::Data(i as usize)).unwrap();
                }
                for _ in 0..iters {
                    black_box(output.read_front().unwrap());
                }
                start.elapsed()
            })
        });

        group.bench_function("round_trip", |b| {
            b.iter(|| {
                writer.push(Message::Data(black_box(1))).unwrap();
                black_box(output.read_front().unwrap());
            })
        });

        writer.close().unwrap();
        let _ = handle.join();
    });
    group.finish();
}
