//! Pull chains: fused lines without synchronization.

mod support;

use areamy::pull::WriterBuffer;
use areamy::work::Writer;
use areamy::{DefaultThread, Message, Pullable, Pushable, Trackable};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use support::{Pass, frame};

type Signal = Trackable<&'static str>;

fn three_lines(c: &mut Criterion) {
    let mut group = c.benchmark_group("pull/three_lines");
    group.throughput(Throughput::Elements(1));

    let source = WriterBuffer::<usize, Signal, DefaultThread>::new();
    let mut writer = Writer::new(&source).unwrap();
    let mut chain = source.then(Pass::new()).then(Pass::new()).then(Pass::new());
    group.bench_function("usize", |b| {
        b.iter(|| {
            writer.push(Message::Data(black_box(1))).unwrap();
            black_box(chain.pull().unwrap());
        })
    });

    let source = WriterBuffer::<Vec<f32>, Signal, DefaultThread>::new();
    let mut writer = Writer::new(&source).unwrap();
    let mut chain = source.then(Pass::new()).then(Pass::new()).then(Pass::new());
    group.bench_function("frame", |b| {
        b.iter_batched(
            frame,
            |frame| {
                writer.push(Message::Data(frame)).unwrap();
                black_box(chain.pull().unwrap());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, three_lines);
criterion_main!(benches);
