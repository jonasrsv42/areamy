
# Areamy

Streaming graphs for Rust where a wrong graph doesn't compile.

Areamy runs low-latency pipelines (audio in, model inference, audio out) across threads, with
blocking and async stages side by side. It fills a role like
[MediaPipe](https://github.com/google-ai-edge/mediapipe), with no dependencies, and runs on an ESP32.

```rust
use areamy::work::Line;
use areamy::{Reads, Via, Writes};

let inputs = Line::of(AddOne::new());
let middle = Line::of(AddOne::new());
let outputs = Line::of(AddOne::new());

let writer = inputs.writer()?;
let outputs = inputs.via(middle)?.via(outputs)?;
let reader = outputs.reader()?;

let mut io = areamy::LineIo::new(writer, reader);
io.push(areamy::Message::Data(1))?;
assert_eq!(io.read()?, areamy::Message::Data(4));
```

`via` moves the upstream into its owner, so wiring reads in data direction and ownership *is*
the scheduling graph.

## What you get

- **Wiring the compiler checks.** Owning a node twice, a scheduling cycle, a type mismatch, an
  unnamed side, or a work node owning an async one: all compile errors.
  [`src/graph/misuse.rs`](src/graph/misuse.rs) pins each one.
- **Three runtimes, one graph.** Blocking `work` threads for CPU-bound stages, an async `poll`
  runtime for I/O (its own executor, routines are plain `async fn`), and `pull`
  segments with no locks or dynamic dispatch for hot paths. One
  [`ThreadBundle`](src/thread/bundle/core.rs) starts them all.
  [`tests/lifecycle.rs`](tests/lifecycle.rs) mixes all three.
- **Threads are types.** `thread_id!(Encoder)` names a thread; `node.stream::<Encoder>()`
  moves a subgraph onto it.
- **Segments travel in-band.** `Flush` and `Marker` signals flow with the data, so "this
  utterance ended" reaches every stage in order. See [`src/message.rs`](src/message.rs).
- **More than lines.** Two-input `Biunion`, two-output `Bifurcation`, fan-in with `via`, fan-out
  with `tee` / `pushes`, and side paths that carry data without scheduling.
- **Backpressure when you want it.** Queues are unbounded by default; `.bounded(n)` on a node
  caps its input and blocks or suspends the producer.
- **Clean teardown.** Closing an input drains and closes everything downstream. An error or
  panic ends its thread and closes its edges, so the rest winds down; `on_first_error` tells you
  once, so you can rebuild and restart.
- **Timeouts in async stages.** `recv_with_timeout`, `send_with_timeout` and `poll::sleep`.

[PRINCIPLES.md](PRINCIPLES.md) explains the design choices behind these.

## Multiple threads

`inputs` and `middle` run on a helper thread; `outputs` is read on the main thread.

```rust
use areamy::work::Line;
use areamy::{Pushes, Reads, Streams, Via, Writes};

areamy::thread_id!(HelperThread);

let inputs = Line::of(AddOne::new());
let middle = Line::of(AddOne::new());
let outputs = Line::of(AddOne::new());

let writer = inputs.writer()?;
let mut middle = inputs.via(middle)?;
middle.pushes(&outputs)?; // crosses threads: data only, no scheduling
let helper_thread = middle.stream::<HelperThread>();

let mut io = areamy::LineIo::new(writer, outputs.reader()?);

std::thread::scope(|s| -> Result<(), areamy::error::Error> {
    let _handle = helper_thread.start(s);
    io.push(areamy::Message::Data(1))?;
    assert_eq!(io.read()?, areamy::Message::Data(4));
    io.close() // lets the helper thread exit before the scope joins it
})?;
```

## Async stages

A routine is a plain `async fn`. A `Flush` ends its loop, and the next segment starts it again:

```rust
use areamy::poll::future::{Input, Receiver, Sender};

async fn double(input: Receiver<usize>, output: Sender<usize>) -> Result<(), Error> {
    while let Input::Data(value) = input.recv().await? {
        output.send(value * 2).await?;
    }
    Ok(())
}
```

Nodes are minted by their thread and wired with the same verbs. `.input()` and `.output()` are
where the async part meets the rest of the program:

```rust
use areamy::poll::future::line::FutureRoutine;
use areamy::{Tees, Via, Writes, poll};

let mut io = poll::Thread::<IoThread>::new();

let inputs = io.line(FutureRoutine::factory(double)).input();
let writer = inputs.writer()?;

let mut outputs = inputs
    .via(io.line(FutureRoutine::factory(double)))?
    .output();
let reader = outputs.tee()?;
io.add(outputs);
```

More: [`tests/example.rs`](tests/example.rs), and a networking-shaped one (socket,
`try_join`, reconnect per segment) in [`tests/async_bidi_test.rs`](tests/async_bidi_test.rs).

## Embedded

Areamy only uses `std` (`thread`, `sync`, …), which ESP-IDF provides. To check that it
cross-compiles for ESP32-S3:

```bash
cargo install espup && espup install && source ~/export-esp.sh
./scripts/cross-check.sh
```
