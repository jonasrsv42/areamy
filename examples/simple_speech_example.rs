//! A simple streaming speech pipeline, pipelined across threads, with backpressure.
//!
//! For low latency, speech recognition commonly pins each stage to its own thread: the
//! frontend (features + voice activity detection), the encoder and the decoder. The stages then
//! run in parallel on consecutive chunks, like an assembly line: while the decoder works on
//! chunk n, the encoder is already on n + 1 and the frontend on n + 2. Latency per chunk stays
//! close to the slowest stage rather than the sum of all three.
//!
//! Live, a pipeline has to keep up with the microphone; if it can't, it's broken. Bounded
//! inputs still earn their keep there by absorbing stalls and blips without growing without
//! limit. Where the source really does outrun the pipeline is offline: evaluating a 10-hour
//! recording read as fast as the disk allows. Unbounded, the early stages would buffer hours of
//! audio waiting for the decoder.
//!
//! This example is that offline case. The recording is read as fast as possible, each stage is
//! a little slower than the one before it, and the main thread is the app reading transcripts:
//!
//! ```text
//! recording ─▶ [frontend + VAD] ─▶ [encoder] ─▶ [decoder] ─▶ app
//!               1 ms / chunk        2 ms         3 ms
//! ```
//!
//! Every input holds at most a few chunks. When the decoder falls behind, the encoder blocks on
//! the decoder's full input, then the frontend on the encoder's, and finally the reader of the
//! recording itself: memory stays flat however long the file, and the whole run goes at the
//! decoder's pace. Each wait prints a "blocked" / "unblocked" pair to stderr (hide them with
//! `2>/dev/null`).
//!
//! Run with `cargo run --example simple_speech_example`. `cargo test` runs it too.

use areamy::error::Error;
use areamy::thread::Join;
use areamy::work::{Line, Reader, Writer};
use areamy::{Closeable, Flush, LineRoutine, Message, Next, Push, Pushable, ThreadStream};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

areamy::thread_id!(FrontendThread, EncoderThread, DecoderThread);

/// Chunks in the recording.
const CHUNKS: usize = 200;

/// Chunks each input holds before its producer waits.
const QUEUE: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Most chunks that can be read from the recording but not yet transcribed: four queues (three
/// stages and the app's), per stage one chunk in its routine and one held for a full output,
/// and the app's read buffer.
const MOST_IN_FLIGHT: usize = 4 * QUEUE.get() + 3 * 2 + 1;

/// Stands in for a model stage: spends `cost` on each chunk, then passes it on.
struct Stage {
    cost: Duration,
    out: VecDeque<usize>,
}

impl Stage {
    fn costing(millis: u64) -> Self {
        Stage {
            cost: Duration::from_millis(millis),
            out: VecDeque::new(),
        }
    }
}

impl areamy::Send<usize> for Stage {
    fn send(&mut self, chunk: usize) -> Result<(), Error> {
        thread::sleep(self.cost);
        self.out.push_back(chunk);
        Ok(())
    }
}

impl Next<usize> for Stage {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.out.pop_front())
    }
}

impl Flush for Stage {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl LineRoutine<usize, usize> for Stage {}

fn main() -> Result<(), Error> {
    // Every input is bounded.
    let mut frontend = Line::builder().bounded(QUEUE).build(Stage::costing(1));
    let mut encoder = Line::builder().bounded(QUEUE).build(Stage::costing(2));
    let mut decoder = Line::builder().bounded(QUEUE).build(Stage::costing(3));
    let app = Line::builder().bounded(QUEUE).build(Stage::costing(0));

    let mut recording = Writer::new(&frontend)?;
    // Across threads: each stage pushes into the next one's input.
    Push::connect(&mut frontend, &encoder)?;
    Push::connect(&mut encoder, &decoder)?;
    Push::connect(&mut decoder, &app)?;
    let mut transcripts = Reader::new(app)?;

    let read_from_file = Arc::new(AtomicUsize::new(0));
    let mut most_in_flight = 0;

    thread::scope(|scope| -> Result<(), Error> {
        let stages = [
            ThreadStream::<FrontendThread>::of(frontend).start(scope),
            ThreadStream::<EncoderThread>::of(encoder).start(scope),
            ThreadStream::<DecoderThread>::of(decoder).start(scope),
        ];

        // The recording is read as fast as the disk allows; a full frontend input makes it wait.
        let reading = {
            let read_from_file = read_from_file.clone();
            scope.spawn(move || -> Result<(), Error> {
                for chunk in 0..CHUNKS {
                    recording.push(Message::Data(chunk))?;
                    read_from_file.fetch_add(1, Ordering::Relaxed);
                }
                // End of file: the close follows the last chunk through every stage.
                recording.close()
            })
        };

        // The app reads transcripts as they come.
        for transcribed in 1..=CHUNKS {
            let chunk = transcripts.read()?;
            let in_flight = read_from_file
                .load(Ordering::Relaxed)
                .saturating_sub(transcribed);
            most_in_flight = most_in_flight.max(in_flight);
            if transcribed % 20 == 0 {
                println!(
                    "transcribed {chunk:?}: {in_flight:>2} chunks read but not yet transcribed"
                );
            }
        }

        reading
            .join()
            .map_err(|_| areamy::fatal!("recording reader panicked"))??;
        for stage in stages {
            if let Join::Error(error) | Join::Panic(error) = stage.join() {
                return Err(error);
            }
        }
        Ok(())
    })?;

    println!(
        "{CHUNKS} chunks, at most {most_in_flight} in flight at once (the queues allow {MOST_IN_FLIGHT})"
    );
    assert!(
        most_in_flight <= MOST_IN_FLIGHT,
        "backpressure let {most_in_flight} chunks pile up"
    );
    Ok(())
}
