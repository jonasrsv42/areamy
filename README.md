
# Areamy

Areamy is a strongly typed runtime for multithreaded streaming graphs. 
See [src/graph](src/graph.rs) for a brief overview.

It serves a purpose similar to https://github.com/google-ai-edge/mediapipe

The areamy repository itself only has the basic building blocks of the runtime.  

## Example


See [tests/example.rs](tests/example.rs)


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
io.push(areamy::Message::Data(2))?;

assert_eq!(io.read()?, areamy::Message::Data(4));
assert_eq!(io.read()?, areamy::Message::Data(5));
```


### Multithreading

The purpose of areamy is to support multithreaded graphs such as the example below. Where 
there are two threads working on the graph. The main thread and a `HelperThread`. In the example below

```
 main thread
     ↑
  outputs
     ↑
  middle   (helper_thread)
     ↑
  inputs   (helper_thread)

```

the graph computation is split across two threads.

```rust
use areamy::work::Line;
use areamy::{Pushes, Reads, Streams, Via, Writes};

areamy::thread_id!(HelperThread);

let inputs = Line::of(AddOne::new());
let middle = Line::of(AddOne::new());
let outputs = Line::of(AddOne::new());

let writer = inputs.writer()?;
let mut middle = inputs.via(middle)?;

// Middle, on the helper thread, pushes into outputs on the main thread.
middle.pushes(&outputs)?;

// Move inputs → middle onto the helper thread.
let helper_thread = middle.stream::<HelperThread>();

let reader = outputs.reader()?;
let mut io = areamy::LineIo::new(writer, reader);

std::thread::scope(|s| -> Result<(), areamy::error::Error> {
    // Start the helper thread.
    let _handle = helper_thread.start(s);

    // Helper thread runs the first two nodes; main thread reads the output.
    io.push(areamy::Message::Data(1))?;
    io.push(areamy::Message::Data(2))?;

    assert_eq!(io.read()?, areamy::Message::Data(4));
    assert_eq!(io.read()?, areamy::Message::Data(5));

    // Close so the helper thread exits before the scope joins it.
    io.close()
})?;
```

### Async poll runtime — drop in where it fits

CPU-bound? Sync work threads. I/O-bound? Poll thread, futures, wakers.
Mix them in the same `ThreadBundle` — same edges, same teardown.

```rust
let routine = FutureRoutine::factory(|input: input::Receiver<usize>, output: output::Sender<usize>| {
    Box::pin(async move {
        let socket = FakeSocket::connect("…").await;
        let writer = async {
            while let Input::Data(v) = input.recv().await? { socket.write(v * 3).await; }
            socket.half_close().await; // flush → EOF for the reader
            Ok::<_, Error>(())
        };
        let reader = async { while let Some(v) = socket.read().await { output.send(v + 1).await?; } Ok(()) };
        areamy::poll::try_join(writer, reader).await?;
        Ok(())
    })
})
.bounded(NonZeroUsize::new(64)); // optional: `send` waits while 64 are queued

let node = io_thread.line(routine).input::<areamy::poll::Sync>().output::<areamy::poll::Sync>();
```

Full networking-shaped example: [tests/async_bidi_test.rs](tests/async_bidi_test.rs).

#### Deadlines

Race input against a wall-clock timeout via `recv_with_timeout`:

```rust
match input.recv_with_timeout(MAX_HOLD).await? {
    Some(Input::Data(v)) => buffer.push(v),
    Some(Input::Flush)   => return,
    None                 => flush(&mut buffer),  // timed out
}
```

For lower-level use, the `timer` in `LineWakers` / `BiunionWakers` (a
`poll::Timer`) exposes `schedule_at(deadline)` — the node gets re-polled at
that `Instant`.

## Embedded targets

Areamy has no external dependencies and only relies on `std` primitives
(`thread`, `sync`, `backtrace`, …) that are available on Espressif's
ESP-IDF Rust target. To verify the library cross-compiles for ESP32-S3,
install the toolchain once:

```bash
cargo install espup
espup install
source ~/export-esp.sh
```

Then run:

```bash
./scripts/cross-check.sh
```

That builds the library against `xtensa-esp32s3-espidf` (with `-Z build-std`,
since Xtensa std isn't pre-built). Add more targets to the script as needed.
