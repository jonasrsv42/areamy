//! The output thread: drains the graph's output edge into the batches the bench hands it.

use super::{Load, Output};
use crate::support::AudioFrame;
use areamy::Message;
use std::hint;
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::{Duration, Instant};

/// Fills each emptied batch from the output edge and hands it back. Drains the edge to its close
/// once the bench stops, so the graph never pushes into a dropped edge. Returns the frames read.
pub(super) fn drain(
    output: Output,
    to_fill: Receiver<Vec<AudioFrame>>,
    filled: SyncSender<Vec<AudioFrame>>,
    load: Load,
) -> usize {
    let mut read = 0;
    while let Ok(mut frames) = to_fill.recv() {
        while frames.len() < load.batch {
            let Ok(message) = output.read_front() else {
                return read;
            };
            if let Message::Data(frame) = message {
                spin(load.output_cost);
                frames.push(frame);
                read += 1;
            }
        }
        if filled.send(frames).is_err() {
            break;
        }
    }
    while let Ok(message) = output.read_front() {
        if let Message::Data(_) = message {
            read += 1;
        }
    }
    read
}

/// Busy for `cost`: sleeping is too coarse for microseconds.
fn spin(cost: Duration) {
    let start = Instant::now();
    while start.elapsed() < cost {
        hint::spin_loop();
    }
}
