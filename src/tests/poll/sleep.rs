//! [poll::sleep] through the real poll loop: the TLS guard installs
//! the polled node's waker, the sleep captures it, and the deadline
//! heap re-polls the node.

use crate::error::Error;
use crate::poll;
use crate::poll::future::line::FutureRoutine;
use crate::poll::future::queue::{Input, InputConsumer, OutputProducer};
use crate::poll::try_join;
use crate::sync::Receiver;
use crate::work::Writer;
use crate::{Closeable, Message, Pushable, ThreadBundle, ThreadId, make_push};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// One sleep quantum for wall-clock assertions.
const STEP: Duration = Duration::from_millis(40);
/// Scheduling slack: timers may fire marginally early and threads
/// resume marginally late; lower bounds subtract this.
const EPSILON: Duration = Duration::from_millis(10);

#[derive(Debug)]
struct SleepThread;
impl ThreadId for SleepThread {}

type BoxFut = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

#[test]
fn sleep_in_routine_delays_output() {
    let mut thread = poll::Thread::<'_, SleepThread>::new();
    let mut node = thread
        .line(FutureRoutine::factory(
            |input: InputConsumer<usize>, output: OutputProducer<usize>| -> BoxFut {
                Box::pin(async move {
                    loop {
                        match input.recv().await? {
                            Input::Data(n) => {
                                poll::sleep(STEP).await?;
                                output.push(n);
                            }
                            Input::Flush => break,
                        }
                    }
                    Ok(())
                })
            },
        ))
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let mut writer = Writer::new(&node).unwrap();
    let output = Receiver::new();
    make_push(&mut node, &output).unwrap();
    thread.add(node);

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        let start = Instant::now();
        writer.push(Message::Data(7)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(7));
        assert!(start.elapsed() >= STEP - EPSILON);

        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });
}

#[test]
fn concurrent_sleeps_wake_independently() {
    let mut thread = poll::Thread::<'_, SleepThread>::new();
    let mut node = thread
        .line(FutureRoutine::factory(
            |input: InputConsumer<usize>, output: OutputProducer<usize>| -> BoxFut {
                // Three overlapping sleeps: completion order proves
                // per-timer wakes (last-wins clobbering would misorder)
                // and total elapsed proves concurrency (serial would
                // need the 6×STEP sum, concurrent only the 3×STEP max).
                let sleeper = |n: u32| {
                    let out = output.clone();
                    async move {
                        poll::sleep(STEP * n).await?;
                        out.push(n as usize);
                        Ok::<_, Error>(())
                    }
                };
                let all = try_join(sleeper(1), try_join(sleeper(2), sleeper(3)));
                Box::pin(async move {
                    all.await?;
                    loop {
                        match input.recv().await? {
                            Input::Data(_) => {}
                            Input::Flush => break,
                        }
                    }
                    Ok(())
                })
            },
        ))
        .input::<poll::Sync>()
        .output::<poll::Sync>();

    let mut writer = Writer::new(&node).unwrap();
    let output = Receiver::new();
    make_push(&mut node, &output).unwrap();
    thread.add(node);

    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    std::thread::scope(|s| {
        let handle = bundle.start(s);

        let start = Instant::now();
        assert_eq!(output.read_front().unwrap(), Message::Data(1));
        assert_eq!(output.read_front().unwrap(), Message::Data(2));
        assert_eq!(output.read_front().unwrap(), Message::Data(3));
        let elapsed = start.elapsed();
        // The longest sleep really waited...
        assert!(elapsed >= STEP * 3 - EPSILON);
        // ...and the three ran concurrently: strictly under the
        // 6×STEP serial sum, with headroom against CI descheduling
        // (order + lower bound already prove per-timer wakes).
        assert!(elapsed < STEP * 6);

        writer.close().unwrap();
        assert!(handle.join().errors().is_empty());
    });
}
