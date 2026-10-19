//! Pull chains: fused lines without synchronization.

#[cfg(feature = "trace")]
compile_error!("benches time with `trace` off: its logs fire on every full edge");

mod common;

use areamy::pull::WriterBuffer;
use areamy::{DefaultThread, Message, Pullable, Pushable, Trackable, Writes};
use common::{Pass, frame};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

type Signal = Trackable<&'static str>;

fn three_lines(c: &mut Criterion) {
    let mut group = c.benchmark_group("pull/three_lines");
    group.throughput(Throughput::Elements(1));

    let source = WriterBuffer::<usize, Signal, DefaultThread>::new();
    let mut writer = source.writer().unwrap();
    let mut chain = source.then(Pass::new()).then(Pass::new()).then(Pass::new());
    group.bench_function("payload=usize", |b| {
        b.iter(|| {
            writer.push(Message::Data(black_box(1))).unwrap();
            black_box(chain.pull().unwrap());
        })
    });

    let source = WriterBuffer::<Vec<f32>, Signal, DefaultThread>::new();
    let mut writer = source.writer().unwrap();
    let mut chain = source.then(Pass::new()).then(Pass::new()).then(Pass::new());
    group.bench_function("payload=frame", |b| {
        b.iter_batched(
            frame,
            // Returned, so criterion drops the frame after the clock stops.
            |frame| {
                writer.push(Message::Data(frame)).unwrap();
                chain.pull().unwrap()
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, three_lines);
criterion_main!(benches);
