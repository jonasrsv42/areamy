//! A graph under bench: its threads, the output thread, and the batches circulating between them.

use super::cpu_clock::CpuClock;
use super::output::drain;
use super::{Graph, Input, Load, Metric};
use crate::support::{AudioFrame, audio_frame};
use areamy::{Closeable, Message, Pushable, ThreadBundleHandle};
use std::mem;
use std::process;
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::thread::{Scope, ScopedJoinHandle};
use std::time::{Duration, Instant};

/// Fail instead of hanging when a batch doesn't come back.
const STALL: Duration = Duration::from_secs(5);

pub(super) struct Running<'s> {
    input: ClosingInput,
    /// The measured thread's clock; none for wall time.
    clock: Option<CpuClock>,
    check: fn(),
    /// Frames pushed so far, for the output thread's count to match.
    pushed: usize,
    /// The frames the next batch pushes.
    held: Vec<AudioFrame>,
    /// Emptied batches, for the output thread to fill.
    emptied: SyncSender<Vec<AudioFrame>>,
    filled: mpsc::Receiver<Vec<AudioFrame>>,
    threads: ThreadBundleHandle<'s>,
    output: ScopedJoinHandle<'s, usize>,
}

impl<'s> Running<'s> {
    pub(super) fn start<'p>(
        s: &'s Scope<'s, 'p>,
        graph: Graph<'p>,
        load: Load,
        metric: Metric,
    ) -> Self {
        // Two batches circulate: one in flight, one held.
        let (emptied, to_fill) = mpsc::sync_channel(2);
        let (to_return, filled) = mpsc::sync_channel(2);
        let threads = graph.threads.start(s);
        let output = graph.output;
        let output = s.spawn(move || drain(output, to_fill, to_return, load));

        let mut running = Running {
            input: ClosingInput(graph.input),
            clock: None,
            check: graph.check,
            pushed: 0,
            held: (0..load.batch).map(|_| audio_frame()).collect(),
            emptied,
            filled,
            threads,
            output,
        };
        running.push_held();
        running.held = (0..load.batch).map(|_| audio_frame()).collect();
        // A thread names itself once it runs; a batch back through the graph proves it has.
        if let Metric::Cpu(thread) = metric {
            running.batch();
            running.clock = Some(CpuClock::of(thread));
        }
        running
    }

    pub(super) fn mark(&self) -> Mark {
        match self.clock {
            None => Mark::Wall(Instant::now()),
            Some(clock) => Mark::Cpu(clock, clock.now()),
        }
    }

    pub(super) fn check(&self) {
        (self.check)();
    }

    /// Push the held batch, then take back the one before it.
    pub(super) fn batch(&mut self) {
        self.push_held();
        self.held = match self.filled.recv_timeout(STALL) {
            Ok(frames) => frames,
            // The output thread saw the edge close: the graph ended, so its threads join.
            Err(RecvTimeoutError::Disconnected) => panic!("the graph closed its output edge"),
            // A lost wake leaves a thread parked that nothing can release, so the scope's join
            // would hang: exit instead.
            Err(RecvTimeoutError::Timeout) => {
                eprintln!("pipeline stalled: no batch back in {STALL:?}");
                process::exit(101);
            }
        };
    }

    fn push_held(&mut self) {
        self.pushed += self.held.len();
        for frame in self.held.drain(..) {
            self.input.0.push(Message::Data(frame)).unwrap();
        }
        let emptied = mem::take(&mut self.held);
        self.emptied.send(emptied).expect("output thread stopped");
    }

    /// Close the graph, join it, and check the output thread read every frame. Runs after criterion
    /// has saved the samples: a failure here voids the run's baseline.
    pub(super) fn finish(self) {
        let Running {
            input,
            pushed,
            held,
            emptied,
            filled,
            threads,
            output,
            ..
        } = self;
        // Let the output thread go: it waits on `emptied` for a batch, then drains the edge to its
        // close.
        drop((input, held, emptied, filled));
        let errors = threads.join().errors();
        assert!(errors.is_empty(), "graph failed: {errors:?}");
        let read = output.join().unwrap();
        assert_eq!(
            read, pushed,
            "the output thread read a different number of frames"
        );
    }
}

/// When a sample started.
pub(super) enum Mark {
    Wall(Instant),
    Cpu(CpuClock, Duration),
}

impl Mark {
    pub(super) fn elapsed(&self) -> Duration {
        match *self {
            Mark::Wall(start) => start.elapsed(),
            Mark::Cpu(clock, start) => clock.now() - start,
        }
    }
}

/// Closes the graph's input when dropped.
struct ClosingInput(Input);

impl Drop for ClosingInput {
    fn drop(&mut self) {
        let _ = self.0.close();
    }
}
