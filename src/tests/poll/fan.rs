//! Fan-in and fan-out: merges, multiple sinks, and direct sinks.

use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Schedule, TryPush};
use crate::poll;
use crate::signal::Trackable;
use crate::sync::Receiver;
use crate::thread::Join;
use crate::wake;
use crate::work;
use crate::{
    Closeable, DefaultThread, Message, Outlet, Pushable, Pushes, Streams, ThreadBundle,
    TryPushable, Via, Writes,
};
use alloc::sync::Arc;
use core::time::Duration;
use std::sync::Mutex;

use super::mock::{Double, IoThread, PollAccumulator, PollDouble};
use crate::poll::future::line::FutureRoutine;
use crate::poll::future::{Input, input, output};

/// Fan-out via cross-thread sync bridge (Mutex) between Node<Sync, Sync> terminals.
///
/// ```text
///                  ┌→ node_b → output_b
/// writer → node_a ─┤
///                  └→ node_c → output_c
/// ```
///
/// Uses SyncBridge (Mutex) for `pushes` edges between async nodes.
/// For zero-Mutex local edges, use `via` fan-in — but that enforces DAG.
/// `pushes` edges allow fan-out at the cost of Mutex.
#[test]
fn async_fan_out_via_sync_bridge() -> Result<(), Error> {
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let mut node_a = async_thread.line(PollDouble::new).input().output();
    let mut node_b = async_thread.line(PollDouble::new).input().output();
    let mut node_c = async_thread.line(PollDouble::new).input().output();

    writer_node.pushes(&node_a)?;
    node_a.pushes(&node_b)?;
    node_a.pushes(&node_c)?;

    let output_b = Receiver::new();
    let output_c = Receiver::new();
    node_b.pushes(&output_b)?;
    node_c.pushes(&output_c)?;

    async_thread.add(node_a);
    async_thread.add(node_b);
    async_thread.add(node_c);

    let sync_thread = writer_node.stream::<DefaultThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // 5 → sync Double → 10 → node_a PollDouble → 20
        //   → node_b PollDouble → 40
        //   → node_c PollDouble → 40
        writer.push(Message::Data(5))?;
        assert_eq!(output_b.read_front()?, Message::Data(40));
        assert_eq!(output_c.read_front()?, Message::Data(40));

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Two independent parent chains merged into one child.
///
/// ```text
/// writer_a → Node<Sync,Async> ─┐
///                                ├→ Node<Async,Sync> → output
/// writer_b → Node<Sync,Async> ─┘
/// ```
///
/// Child drains from both parents. Output receives doubled values from both.
#[test]
fn merge_two_parents_into_child() -> Result<(), Error> {
    let mut writer_a_node = work::Line::of(Double::new());
    let mut writer_a = writer_a_node.writer()?;

    let mut writer_b_node = work::Line::of(Double::new());
    let mut writer_b = writer_b_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let parent_a = async_thread.line(PollDouble::new).input();
    writer_a_node.pushes(&parent_a)?;

    let parent_b = async_thread.line(PollDouble::new).input();
    writer_b_node.pushes(&parent_b)?;

    let child = parent_a.via(async_thread.line(PollDouble::new))?;
    let mut child = parent_b.via(child)?.output();

    let output = Receiver::new();
    child.pushes(&output)?;

    async_thread.add(child);

    // Independent blocking writers need separate sync threads —
    // work() calls read_front() which blocks, preventing other
    // writers on the same thread from being serviced.
    let sync_a = writer_a_node.stream::<DefaultThread>();
    let sync_b = writer_b_node.stream::<DefaultThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_a).add(sync_b).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // writer_a: 5 → Double → 10 → PollDouble → 20 → PollDouble → 40
        writer_a.push(Message::Data(5))?;
        assert_eq!(output.read_front()?, Message::Data(40));

        // writer_b: 3 → Double → 6 → PollDouble → 12 → PollDouble → 24
        writer_b.push(Message::Data(3))?;
        assert_eq!(output.read_front()?, Message::Data(24));

        writer_a.close()?;
        writer_b.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Three parents merged into one linked node, then child for sync output.
///
/// ```text
/// writer_a → Node<Sync,Async> ─┐
///                                │
/// writer_b → Node<Sync,Async> ──┼→ Node<Async,Deferred> → Node<Async,Sync> → output
///                                │
/// writer_c → Node<Sync,Async> ─┘
/// ```
#[test]
fn merge_three_parents_via_linked() -> Result<(), Error> {
    let mut writer_a_node = work::Line::of(Double::new());
    let mut writer_a = writer_a_node.writer()?;

    let mut writer_b_node = work::Line::of(Double::new());
    let mut writer_b = writer_b_node.writer()?;

    let mut writer_c_node = work::Line::of(Double::new());
    let mut writer_c = writer_c_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let parent_a = async_thread.line(PollDouble::new).input();
    writer_a_node.pushes(&parent_a)?;

    let parent_b = async_thread.line(PollDouble::new).input();
    writer_b_node.pushes(&parent_b)?;

    let parent_c = async_thread.line(PollDouble::new).input();
    writer_c_node.pushes(&parent_c)?;

    // All three parents merged into one linked node
    let linked = parent_a.via(async_thread.line(PollDouble::new))?;
    let linked = parent_b.via(linked)?;
    let linked = parent_c.via(linked)?;

    let mut child = linked.via(async_thread.line(PollDouble::new))?.output();

    let output = Receiver::new();
    child.pushes(&output)?;

    async_thread.add(child);

    // Independent blocking writers need separate sync threads.
    let sync_a = writer_a_node.stream::<DefaultThread>();
    let sync_b = writer_b_node.stream::<DefaultThread>();
    let sync_c = writer_c_node.stream::<DefaultThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_a).add(sync_b).add(sync_c).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // writer_a: 1 → Double → 2 → PollDouble → 4
        //   → linked PollDouble → 8 → child PollDouble → 16
        writer_a.push(Message::Data(1))?;
        assert_eq!(output.read_front()?, Message::Data(16));

        // writer_c: 2 → Double → 4 → PollDouble → 8
        //   → linked PollDouble → 16 → child PollDouble → 32
        writer_c.push(Message::Data(2))?;
        assert_eq!(output.read_front()?, Message::Data(32));

        writer_a.close()?;
        writer_b.close()?;
        writer_c.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Sink: Deferred output, data discarded via Null.
#[test]
fn node_sink_deferred_output() -> Result<(), Error> {
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let parent = async_thread.line(PollDouble::new).input();
    writer_node.pushes(&parent)?;

    let sink = parent.via(async_thread.line(PollDouble::new))?;
    async_thread.add(sink);

    let sync_thread = writer_node.stream::<DefaultThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        writer.push(Message::Data(5))?;

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Multiple async sink nodes with PollAccumulator to verify data arrives.
///
/// ```text
/// writer_a → Node<Sync,Async> (PollDouble) ─┐
///                                             ├→ Node<Async,Deferred> (sink_1, PollAccumulator)
/// writer_b → Node<Sync,Async> (PollDouble) ─┘
///
/// writer_c → Node<Sync,Async> (PollDouble) ──→ Node<Async,Deferred> (sink_2, PollAccumulator)
/// ```
///
/// Both sinks accumulate values. Test verifies correct values arrive.
#[test]
fn async_only_multiple_sinks() -> Result<(), Error> {
    let mut writer_a_node = work::Line::of(Double::new());
    let mut writer_a = writer_a_node.writer()?;

    let mut writer_b_node = work::Line::of(Double::new());
    let mut writer_b = writer_b_node.writer()?;

    let mut writer_c_node = work::Line::of(Double::new());
    let mut writer_c = writer_c_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let parent_a = async_thread.line(PollDouble::new).input();
    writer_a_node.pushes(&parent_a)?;

    let parent_b = async_thread.line(PollDouble::new).input();
    writer_b_node.pushes(&parent_b)?;

    let parent_c = async_thread.line(PollDouble::new).input();
    writer_c_node.pushes(&parent_c)?;

    let collected_1 = Arc::new(Mutex::new(Vec::new()));
    let collected_2 = Arc::new(Mutex::new(Vec::new()));

    // Sink 1: merges a + b, accumulates doubled values
    // writer_a: 1 → Double → 2 → PollDouble → 4 → sink_1 accumulates 4
    // writer_b: 2 → Double → 4 → PollDouble → 8 → sink_1 accumulates 8
    let sink_1 = parent_a.via(async_thread.line({
        let c = collected_1.clone();
        move |_w| PollAccumulator::new(c)
    }))?;
    let sink_1 = parent_b.via(sink_1)?;
    async_thread.add(sink_1);

    // Sink 2: owns c, accumulates doubled values
    // writer_c: 3 → Double → 6 → PollDouble → 12 → sink_2 accumulates 12
    let sink_2 = parent_c.via(async_thread.line({
        let c = collected_2.clone();
        move |_w| PollAccumulator::new(c)
    }))?;
    async_thread.add(sink_2);

    let sync_a = writer_a_node.stream::<DefaultThread>();
    let sync_b = writer_b_node.stream::<DefaultThread>();
    let sync_c = writer_c_node.stream::<DefaultThread>();

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_a).add(sync_b).add(sync_c).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        writer_a.push(Message::Data(1))?;
        writer_b.push(Message::Data(2))?;
        writer_c.push(Message::Data(3))?;

        // Fast close drops unprocessed `send()` data — push Flush so the
        // routines drain pipelined values before the close cascade.
        writer_a.push(Message::Flush("end".into()))?;
        writer_b.push(Message::Flush("end".into()))?;
        writer_c.push(Message::Flush("end".into()))?;

        writer_a.close()?;
        writer_b.close()?;
        writer_c.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());

        // Verify sink_1 received doubled values from a and b
        let mut vals_1 = collected_1.lock().unwrap().clone();
        vals_1.sort();
        assert_eq!(vals_1, vec![4, 8]);

        // Verify sink_2 received doubled value from c
        let vals_2 = collected_2.lock().unwrap().clone();
        assert_eq!(vals_2, vec![12]);

        Ok(())
    })
}

/// Not `Clone`: a single output edge takes the move, so a node with one needs no `Clone`.
struct Moved(&'static str);

/// A caller-provided sink: forwards moved data and signals its close, so a
/// test blocks on the channels instead of polling shared state.
struct Collect {
    items: std::sync::mpsc::Sender<Moved>,
    closed: std::sync::mpsc::Sender<()>,
}

impl Connection for Collect {}

impl Outlet for Collect {
    type DataType = Moved;
    type SignalType = Trackable<&'static str>;
}

impl TryPushable for Collect {
    fn try_push(
        &mut self,
        msg: Message<Moved, Self::SignalType>,
    ) -> Result<TryPush<Message<Moved, Self::SignalType>>, Error> {
        if let Message::Data(data) = msg {
            self.items.send(data).unwrap();
        }
        Ok(TryPush::Pushed)
    }
}

/// Never refuses, so it never owes a wake.
impl Schedule for Collect {
    fn schedule(&mut self, _waker: wake::Waker) {}
}

impl Closeable for Collect {
    fn close(&mut self) -> Result<(), Error> {
        self.closed.send(()).unwrap();
        Ok(())
    }
}

/// A direct sink is the node's single consumer: output moves into it, never cloned, and node
/// teardown closes it.
#[test]
fn node_direct_sink_moves_output_and_closes() -> Result<(), Error> {
    let (items, received) = std::sync::mpsc::channel();
    let (closed, close_signal) = std::sync::mpsc::channel();

    let mut async_thread = poll::Thread::<IoThread>::new();
    let node = async_thread
        .line(FutureRoutine::factory(
            |input: input::Receiver<Moved>, output: output::Sender<Moved>| {
                Box::pin(async move {
                    loop {
                        match input.recv().await? {
                            Input::Data(data) => output.send(data).await?,
                            Input::Flush => return Ok(()),
                        }
                    }
                })
            },
        ))
        .input()
        .sink(Collect { items, closed });
    let mut writer = node.writer()?;
    async_thread.add(node);

    std::thread::scope(|s| -> Result<(), Error> {
        let handle = async_thread.start(s);

        writer.push(Message::Data(Moved("a")))?;
        writer.push(Message::Data(Moved("b")))?;

        // Wait for delivery before closing: close is a fast close, so
        // in-flight input would be dropped, not flushed.
        let timeout = Duration::from_secs(5);
        assert_eq!(received.recv_timeout(timeout).unwrap().0, "a");
        assert_eq!(received.recv_timeout(timeout).unwrap().0, "b");

        writer.close()?;
        assert!(matches!(handle.join(), Join::Ok), "clean node exit",);
        close_signal
            .recv_timeout(timeout)
            .expect("node teardown closes the sink");
        Ok(())
    })
}
