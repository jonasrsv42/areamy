//! Work graphs: blocking lines scheduled by their downstreams.

mod support;

use areamy::sync::Receiver;
use areamy::work::{Biunion, Line, Reader, Writer};
use areamy::{
    At, Closeable, Message, Pushable, Pushes, Reads, Streams, Trackable, Via, Writes, biunion,
};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::thread;
use std::time::Instant;
use support::{Merge, Pass, frame};

areamy::thread_id!(Helper);

type Signal = Trackable<&'static str>;

/// Three pass-through lines chained with `via` on one thread.
fn three_lines<T: Clone + Send + Sync + 'static>() -> (Writer<'static, T>, Reader<'static, T>) {
    let first = Line::of(Pass::<T>::new());
    let writer = first.writer().unwrap();
    let third = first
        .via(Line::of(Pass::new()))
        .unwrap()
        .via(Line::of(Pass::new()))
        .unwrap();
    (writer, third.reader().unwrap())
}

fn pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("work/three_lines");
    group.throughput(Throughput::Elements(1));

    let (mut writer, mut reader) = three_lines::<usize>();
    group.bench_function("usize", |b| {
        b.iter(|| {
            writer.push(Message::Data(black_box(1))).unwrap();
            black_box(reader.read().unwrap());
        })
    });

    let (mut writer, mut reader) = three_lines::<Vec<f32>>();
    group.bench_function("frame", |b| {
        b.iter_batched(
            frame,
            |frame| {
                writer.push(Message::Data(frame)).unwrap();
                black_box(reader.read().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

/// One line pushing each frame into `outputs` sinks: the owning downstream plus `pushes` edges.
fn fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("work/fan_out_frame");
    group.throughput(Throughput::Elements(1));

    for outputs in [1usize, 3] {
        let mut source = Line::of(Pass::<Vec<f32>>::new());
        let mut writer = source.writer().unwrap();
        let extras: Vec<Receiver<Vec<f32>, Signal>> =
            (1..outputs).map(|_| Receiver::new()).collect();
        for extra in &extras {
            source.pushes(extra).unwrap();
        }
        let owner = source.via(Line::of(Pass::new())).unwrap();
        let mut reader = owner.reader().unwrap();

        group.bench_function(format!("{outputs}_outputs"), |b| {
            b.iter_batched(
                frame,
                |frame| {
                    writer.push(Message::Data(frame)).unwrap();
                    black_box(reader.read().unwrap());
                    for extra in &extras {
                        black_box(extra.poll().unwrap());
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Biunion fed on both sides, read on this thread.
fn biunion(c: &mut Criterion) {
    let mut group = c.benchmark_group("work/biunion");
    group.throughput(Throughput::Elements(2));

    let node = Biunion::of(Merge::<usize>::new());
    let mut left = node.at_ref::<biunion::Left>().writer().unwrap();
    let mut right = node.at_ref::<biunion::Right>().writer().unwrap();
    let mut reader = node.reader().unwrap();

    group.bench_function("both_sides", |b| {
        b.iter(|| {
            left.push(Message::Data(black_box(1))).unwrap();
            right.push(Message::Data(black_box(2))).unwrap();
            black_box(reader.read().unwrap());
            black_box(reader.read().unwrap());
        })
    });
    group.finish();
}

/// First two lines on a helper thread pushing into the last line on this thread.
fn helper_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("work/helper_thread");
    group.throughput(Throughput::Elements(1));

    let first = Line::of(Pass::<usize>::new());
    let last = Line::of(Pass::new());
    let mut writer = first.writer().unwrap();
    let mut second = first.via(Line::of(Pass::new())).unwrap();
    second.pushes(&last).unwrap();
    let helper = second.stream::<Helper>();
    let mut reader = last.reader().unwrap();

    thread::scope(|s| {
        let handle = helper.start(s);
        group.bench_function("burst", |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                for i in 0..iters {
                    writer.push(Message::Data(i as usize)).unwrap();
                }
                for _ in 0..iters {
                    black_box(reader.read().unwrap());
                }
                start.elapsed()
            })
        });
        writer.close().unwrap();
        let _ = handle.join();
    });
    group.finish();
}

criterion_group!(benches, pipeline, fan_out, biunion, helper_thread);
criterion_main!(benches);
