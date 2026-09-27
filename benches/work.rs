//! Work graphs: blocking lines scheduled by their children.

mod support;

use areamy::sync::Receiver;
use areamy::work::{self, Line, Reader, Writer};
use areamy::{Closeable, Message, Push, Pushable, ThreadStream, Trackable};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::thread;
use std::time::Instant;
use support::{Pass, frame};

areamy::thread_id!(Helper);

type Signal = Trackable<&'static str>;

/// Three pass-through lines chained with bidi on one thread.
fn three_lines<T: Clone + Send + Sync + 'static>() -> (Writer<'static, T>, Reader<'static, T>) {
    let first = Line::of(Pass::<T>::new());
    let mut second = Line::of(Pass::new());
    let mut third = Line::of(Pass::new());
    let writer = Writer::new(&first).unwrap();
    work::Bidi::connect(first, &mut second).unwrap();
    work::Bidi::connect(second, &mut third).unwrap();
    (writer, Reader::new(third).unwrap())
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

/// One line pushing each frame into `outputs` sinks: the owning child plus plain edges.
fn fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("work/fan_out_frame");
    group.throughput(Throughput::Elements(1));

    for outputs in [1usize, 3] {
        let mut source = Line::of(Pass::<Vec<f32>>::new());
        let mut writer = Writer::new(&source).unwrap();
        let extras: Vec<Receiver<Vec<f32>, Signal>> =
            (1..outputs).map(|_| Receiver::new()).collect();
        for extra in &extras {
            Push::connect(&mut source, extra).unwrap();
        }
        let mut owner = Line::of(Pass::new());
        work::Bidi::connect(source, &mut owner).unwrap();
        let mut reader = Reader::new(owner).unwrap();

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

/// First two lines on a helper thread pushing into the last line on this thread.
fn helper_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("work/helper_thread");
    group.throughput(Throughput::Elements(1));

    let first = Line::of(Pass::<usize>::new());
    let mut second = Line::of(Pass::new());
    let last = Line::of(Pass::new());
    let mut writer = Writer::new(&first).unwrap();
    work::Bidi::connect(first, &mut second).unwrap();
    Push::connect(&mut second, &last).unwrap();
    let helper = ThreadStream::<Helper>::of(second);
    let mut reader = Reader::new(last).unwrap();

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

criterion_group!(benches, pipeline, fan_out, helper_thread);
criterion_main!(benches);
