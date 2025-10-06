
# Areamy

Areamy is a strongly typed runtime for multithreaded streaming graphs. 
See [src/connect/graph](src/connect/graph.rs) for a brief overview.

It serves a purpose similar to https://github.com/google-ai-edge/mediapipe

The areamy repository itself only has the basic building blocks of the runtime.  

## Example


See [tests/example.rs](tests/example.rs)


```rust

let mut in_node = areamy::sync::make_line(AddOne::new())?;
let mut middle_node = areamy::sync::make_line(AddOne::new())?;
let mut out_node = areamy::sync::make_line(AddOne::new())?;

let source = areamy::sync::Source::<usize>::of(in_node.clone())?;

areamy::sync::Connect::<usize>::bidi(&mut in_node, &mut middle_node)?;
areamy::sync::Connect::<usize>::bidi(&mut middle_node, &mut out_node)?;

let sink = areamy::sync::Sink::new(out_node.workable(), out_node.output())?;

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

let mut in_node = areamy::sync::make_line(AddOne::new())?;
let mut middle_node = areamy::sync::make_line(AddOne::new())?;
let mut out_node = areamy::sync::make_line(AddOne::new())?;

let source = areamy::sync::Source::<usize>::of(in_node.clone())?;
areamy::sync::Connect::<usize>::bidi(&mut in_node, &mut middle_node)?;

let mut helper_thread = areamy::ThreadStream::<HelperThread>::new();

// Now helper thread will work on the middle_node subgraph.
areamy::make_work(&middle_node, helper_thread.as_mut())?;

// Ensure that middle node, using the `HelperThread` pushes data into out node.
areamy::sync::Connect::<usize>::push(&mut middle_node, &out_node)?;

let sink = areamy::sync::Sink::new(out_node.workable(), out_node.output())?;
let mut reader = areamy::LineReader::new(source, sink);

// Start the helper thread.
helper_thread.start()?;

// Helper thread will run the computation in the first two nodes.
reader.push(areamy::Message::Data(1))?;
reader.push(areamy::Message::Data(2))?;

// Main thread runs computation in the final output node.
assert_eq!(reader.read().unwrap(), areamy::Message::Data(4));
assert_eq!(reader.read().unwrap(), areamy::Message::Data(5));

```
