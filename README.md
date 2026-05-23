
# Areamy

Areamy is a strongly typed runtime for multithreaded streaming graphs. 
See [src/connect/graph](src/connect/graph.rs) for a brief overview.

It serves a purpose similar to https://github.com/google-ai-edge/mediapipe

The areamy repository itself only has the basic building blocks of the runtime.  

## Example


See [tests/example.rs](tests/example.rs)


```rust
let in_node = areamy::work::make_line(AddOne::new());
let mut middle_node = areamy::work::make_line(AddOne::new());
let mut out_node = areamy::work::make_line(AddOne::new());

let source = areamy::work::Source::<usize>::of(&in_node)?;

areamy::work::Connect::<usize>::bidi(in_node, &mut middle_node)?;
areamy::work::Connect::<usize>::bidi(middle_node, &mut out_node)?;

let sink = areamy::work::Sink::new(out_node)?;

let mut reader = areamy::LineReader::new(source, sink);

reader.push(areamy::Message::Data(1))?;
reader.push(areamy::Message::Data(2))?;

assert_eq!(reader.read().unwrap(), areamy::Message::Data(4));
assert_eq!(reader.read().unwrap(), areamy::Message::Data(5));
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
let in_node = areamy::work::make_line(AddOne::new());
let mut middle_node = areamy::work::make_line(AddOne::new());
let out_node = areamy::work::make_line(AddOne::new());

let source = areamy::work::Source::<usize>::of(&in_node)?;
areamy::work::Connect::<usize>::bidi(in_node, &mut middle_node)?;

let mut helper_thread = areamy::ThreadStream::<HelperThread>::new();

// Wire the middle node to push into the out node on the main thread.
areamy::work::Connect::<usize>::push(&mut middle_node, &out_node)?;

// Move the middle node onto the helper thread.
areamy::make_work(middle_node, &mut helper_thread)?;

let sink = areamy::work::Sink::new(out_node)?;
let mut reader = areamy::LineReader::new(source, sink);

// Start the helper thread.
let _handle = helper_thread.start();

// Helper thread runs the first two nodes; main thread runs the sink.
reader.push(areamy::Message::Data(1))?;
reader.push(areamy::Message::Data(2))?;

assert_eq!(reader.read().unwrap(), areamy::Message::Data(4));
assert_eq!(reader.read().unwrap(), areamy::Message::Data(5));
```

### Async poll runtime — drop in where it fits

CPU-bound? Sync work threads. I/O-bound? Poll thread, futures, wakers.
Mix them in the same `ThreadBundle` — same edges, same teardown.

```rust
let socket = FakeSocket::connect("…").await;
Join::join([
    async move { while let Some(v) = socket.read().await { out.push(v + 1); } },
    async move { while let Input::Data(v) = input.recv().await? { socket.write(v * 3).await; } },
]).await
```

Full networking-shaped example: [tests/poll_bidi_example_test.rs](tests/poll_bidi_example_test.rs).

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
