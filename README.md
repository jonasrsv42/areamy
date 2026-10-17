
# Areamy

<p align="center"><img src="assets/stream.svg" alt="A signal flowing through a streaming graph" width="100%"></p>

Streaming graphs for Rust where a wrong graph doesn't compile.

Areamy runs low-latency pipelines across threads: audio, video, sensor readings or any other
stream goes in, through stages such as model inference, and out, with blocking and async stages
side by side. It fills a role like
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

- **Compile-checked wiring**: [misuse cases](src/graph/misuse.rs)
- **Blocking, async and pull runtimes in one graph**: [tests/lifecycle.rs](tests/lifecycle.rs)
- **Plain `async fn` routines**: [tests/async_bidi_test.rs](tests/async_bidi_test.rs)
- **Threads as types**: [Multiple threads](#multiple-threads)
- **In-band segments** (`Flush`, `Marker`): [src/message.rs](src/message.rs)
- **Fan-in, fan-out, two-input and two-output nodes**: [`Wiring`](src/graph/misuse.rs)
- **Opt-in backpressure**: [src/tests/backpreassure.rs](src/tests/backpreassure.rs)
- **Clean teardown and restart**: [src/thread/bundle/core.rs](src/thread/bundle/core.rs)
- **Async timeouts and sleep**: [src/poll/sleep.rs](src/poll/sleep.rs)
- **Zero dependencies, runs on ESP32**: [Embedded](#embedded)

Why it's built this way: [PRINCIPLES.md](PRINCIPLES.md).

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
use areamy::poll::{self, FutureRoutine};
use areamy::{Tees, Via, Writes};

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

Areamy only uses `std` (`thread`, `sync`, …), which ESP-IDF provides. Its default thread stack
is a few KB, so size deep threads when spawning: `thread.with().stack_size(32 * 1024)`. To
check that it cross-compiles for ESP32-S3:

```bash
cargo install espup && espup install && source ~/export-esp.sh
./scripts/cross-check.sh
```
