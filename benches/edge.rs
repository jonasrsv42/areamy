//! Raw sync edge: the queue every work connection is built on.

use areamy::sync::Receiver;
use areamy::{Message, Trackable};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::thread;
use std::time::Instant;

type Signal = Trackable<&'static str>;

const BOUND: NonZeroUsize = NonZeroUsize::new(64).unwrap();

fn same_thread(c: &mut Criterion) {
    let receiver = Receiver::<u64, Signal>::new();
    let sender = receiver.sender();

    let mut group = c.benchmark_group("edge");
    group.throughput(Throughput::Elements(1));
    group.bench_function("push_poll_same_thread", |b| {
        b.iter(|| {
            sender.push_back(Message::Data(black_box(1))).unwrap();
            black_box(receiver.poll().unwrap());
        })
    });
    group.finish();
}

fn cross_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge");
    group.throughput(Throughput::Elements(1));

    // Producer thread bursts `iters` messages; the consumer blocks on each.
    group.bench_function("burst_cross_thread", |b| {
        b.iter_custom(|iters| {
            let receiver = Receiver::<u64, Signal>::new();
            let sender = receiver.sender();
            let start = Instant::now();
            let producer = thread::spawn(move || {
                for i in 0..iters {
                    sender.push_back(Message::Data(i)).unwrap();
                }
            });
            for _ in 0..iters {
                black_box(receiver.read_front().unwrap());
            }
            let elapsed = start.elapsed();
            producer.join().unwrap();
            elapsed
        })
    });

    // Same burst, but the producer blocks whenever 64 messages are queued.
    group.bench_function("bounded_burst_cross_thread", |b| {
        b.iter_custom(|iters| {
            let receiver = Receiver::<u64, Signal>::bounded(BOUND);
            let sender = receiver.sender();
            let start = Instant::now();
            let producer = thread::spawn(move || {
                for i in 0..iters {
                    sender.push_back(Message::Data(i)).unwrap();
                }
            });
            for _ in 0..iters {
                black_box(receiver.read_front().unwrap());
            }
            let elapsed = start.elapsed();
            producer.join().unwrap();
            elapsed
        })
    });

    // One message there and back through an echo thread.
    group.bench_function("ping_pong", |b| {
        let to_echo = Receiver::<u64, Signal>::new();
        let from_echo = Receiver::<u64, Signal>::new();
        let to_echo_sender = to_echo.sender();
        let from_echo_sender = from_echo.sender();

        thread::scope(|s| {
            s.spawn(move || {
                while let Ok(message) = to_echo.read_front() {
                    if from_echo_sender.push_back(message).is_err() {
                        break;
                    }
                }
            });
            b.iter(|| {
                to_echo_sender
                    .push_back(Message::Data(black_box(1)))
                    .unwrap();
                black_box(from_echo.read_front().unwrap());
            });
            drop(to_echo_sender);
        });
    });
    group.finish();
}

criterion_group!(benches, same_thread, cross_thread);
criterion_main!(benches);
