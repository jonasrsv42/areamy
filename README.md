
# Areamy

Areamy is a strongly typed runtime for multithreaded streaming graphs. 
See [src/graph](src/graph.rs) for a brief overview.

It serves a purpose similar to https://github.com/google-ai-edge/mediapipe

The areamy repository itself only has the basic building blocks of the runtime.  

## Example


See [tests/example.rs](tests/example.rs)


```rust
let in_node = areamy::work::Line::of(AddOne::new());
let mut middle_node = areamy::work::Line::of(AddOne::new());
let mut out_node = areamy::work::Line::of(AddOne::new());

let writer = areamy::work::Writer::<usize>::of(&in_node)?;

areamy::work::Bidi::<usize>::connect(in_node, &mut middle_node)?;
areamy::work::Bidi::<usize>::connect(middle_node, &mut out_node)?;

let reader = areamy::work::Reader::new(out_node)?;

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
    node
     ↑
    node   (helper_thread)
     ↑
    node   (helper_thread)

```

the graph computation is split across two threads.

```rust
areamy::thread_id!(HelperThread);

let in_node = areamy::work::Line::of(AddOne::new());
let mut middle_node = areamy::work::Line::of(AddOne::new());
let out_node = areamy::work::Line::of(AddOne::new());

let writer = areamy::work::Writer::<usize>::of(&in_node)?;
areamy::work::Bidi::<usize>::connect(in_node, &mut middle_node)?;

// Wire the middle node to push into the out node on the main thread.
areamy::Push::<usize>::connect(&mut middle_node, &out_node)?;

// Move the middle node onto the helper thread.
let helper_thread = areamy::ThreadStream::<HelperThread>::of(middle_node);

let reader = areamy::work::Reader::new(out_node)?;
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
let routine = FutureRoutine::factory(|input: InputConsumer<usize>, output: OutputProducer<usize>| {
    Box::pin(async move {
        let socket = FakeSocket::connect("…").await;
        let writer = async {
            while let Input::Data(v) = input.recv().await? { socket.write(v * 3).await; }
            socket.half_close().await; // flush → EOF for the reader
            Ok::<_, Error>(())
        };
        let reader = async { while let Some(v) = socket.read().await { output.push(v + 1); } Ok(()) };
        areamy::poll::try_join(writer, reader).await?;
        Ok(())
    })
});

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

For lower-level use, every `Waker` exposes `schedule_at(deadline)` —
the node gets re-polled at that `Instant`.

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
