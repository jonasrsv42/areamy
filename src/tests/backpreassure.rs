//! Backpressure end to end: a stalled consumer stops its producers, through every kind of node.
//!
//! The threaded tests push and read with deadlines, so a lost wake fails instead of hanging.

mod future;
mod hybrid;
mod poll;
mod work;

use crate::edge::sync::Receiver;
use crate::graph::{TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Trackable;
use crate::work::Writer;
use core::num::NonZeroUsize;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::time::Duration;
use std::thread;
use std::time::Instant;

type Signal = Trackable<&'static str>;
type Msg = Message<usize, Signal>;

const TIMEOUT: Duration = Duration::from_secs(5);

fn one() -> NonZeroUsize {
    NonZeroUsize::MIN
}

/// Push, retrying while the edge is full.
fn push(writer: &mut Writer<'_, usize, Signal>, mut message: Msg) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        match writer.try_push(message).unwrap() {
            TryPush::Pushed => return,
            TryPush::Full(back) | TryPush::Stuck(back) => message = back,
        }
        assert!(Instant::now() < deadline, "timed out pushing");
        thread::sleep(Duration::from_millis(1));
    }
}

/// The next message, or `None` once the edge is closed and empty.
fn read(output: &Receiver<usize, Signal>) -> Option<Msg> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        match output.poll() {
            Ok(Some(message)) => return Some(message),
            Ok(None) => {}
            Err(e) if e.is_closed() => return None,
            Err(e) => panic!("read failed: {e:?}"),
        }
        assert!(Instant::now() < deadline, "timed out reading");
        thread::sleep(Duration::from_millis(1));
    }
}

fn read_n(output: &Receiver<usize, Signal>, count: usize) -> Vec<Msg> {
    (0..count)
        .map(|_| read(output).expect("closed before all arrived"))
        .collect()
}

/// `count` once it reaches `target` (or the deadline passes) and 50ms more: past the target means
/// the pipeline didn't stall there. Never panics, so the caller can tear down first.
fn settled(count: &AtomicUsize, target: usize) -> usize {
    let deadline = Instant::now() + TIMEOUT;
    while count.load(Ordering::SeqCst) < target && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));
    count.load(Ordering::SeqCst)
}

/// `count` once it stops growing: nonzero and unchanged for 50ms, or at the deadline. Never
/// panics, so the caller can tear down first.
fn stopped(count: &AtomicUsize) -> usize {
    let deadline = Instant::now() + TIMEOUT;
    let mut last = 0;
    loop {
        thread::sleep(Duration::from_millis(50));
        let now = count.load(Ordering::SeqCst);
        if (now == last && now > 0) || Instant::now() >= deadline {
            return now;
        }
        last = now;
    }
}

fn data(values: impl IntoIterator<Item = usize>) -> Vec<Msg> {
    values.into_iter().map(Message::Data).collect()
}
