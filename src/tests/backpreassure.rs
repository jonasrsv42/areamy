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
use std::num::NonZeroUsize;
use std::thread;
use std::time::{Duration, Instant};

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

fn read_all(output: &Receiver<usize, Signal>) -> Vec<Msg> {
    std::iter::from_fn(|| read(output)).collect()
}

fn read_n(output: &Receiver<usize, Signal>, count: usize) -> Vec<Msg> {
    (0..count)
        .map(|_| read(output).expect("closed before all arrived"))
        .collect()
}

fn data(values: impl IntoIterator<Item = usize>) -> Vec<Msg> {
    values.into_iter().map(Message::Data).collect()
}
