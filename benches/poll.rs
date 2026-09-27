//! Poll runtime: sync → `FutureRoutine` line → sync.

mod support;

use areamy::error::Error;
use areamy::poll::future::line::FutureRoutine;
use areamy::poll::future::queue::{Input, InputConsumer, OutputProducer};
use areamy::sync::Receiver;
use areamy::work::{Line, Writer};
use areamy::{
    Closeable, DefaultThread, Message, Push, Pushable, ThreadBundle, ThreadStream, Trackable, poll,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::thread;
use std::time::Instant;
use support::Pass;

areamy::thread_id!(IoThread);

type Signal = Trackable<&'static str>;

fn sync_poll_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/sync_poll_sync");
    group.throughput(Throughput::Elements(1));

    let mut writer_node = Line::of(Pass::<usize>::new());
    let mut writer = Writer::new(&writer_node).unwrap();

    let mut io_thread = poll::Thread::<'_, IoThread>::new();
    let routine = FutureRoutine::factory(
        |input: InputConsumer<usize>, output: OutputProducer<usize>| {
            Box::pin(async move {
                while let Input::Data(value) = input.recv().await? {
                    output.push(value);
                }
                Ok::<_, Error>(())
            })
        },
    );
    let mut node = io_thread
        .line(routine)
        .input::<poll::Sync>()
        .output::<poll::Sync>();
    Push::connect(&mut writer_node, &node).unwrap();
    let output = Receiver::<usize, Signal>::new();
    Push::connect(&mut node, &output).unwrap();
    io_thread.add(node);

    let mut bundle = ThreadBundle::new();
    bundle
        .add(ThreadStream::<DefaultThread>::of(writer_node))
        .add(io_thread);

    thread::scope(|s| {
        let handle = bundle.start(s);

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

criterion_group!(benches, sync_poll_sync);
criterion_main!(benches);
