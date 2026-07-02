//! Integration tests for mixed sync + async graphs.

use crate::error::Error;
use crate::marker::Connection;
use crate::node::Name;
use crate::poll;
use crate::signal::Trackable;
use crate::sync::Receiver;
use crate::{
    Closeable, Message, Pushable, ThreadBundle, ThreadId, ThreadStream, make_push, make_work,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Routine that queues input in send via waker-aware Queue, then
/// doubles it in poll. Send wakes Work via the queue's waker.
use crate::connect::waker::{self as waker};
use crate::poll::LineWakers;
use crate::poll::future::line::FutureRoutine;
use crate::poll::future::queue::{Input, InputConsumer, InputQueue, OutputProducer, OutputQueue};

struct PollDouble {
    input: InputQueue<usize>,
    output: OutputQueue<usize>,
}

impl PollDouble {
    fn new(wakers: LineWakers) -> Self {
        Self {
            // work waker on InputQueue so `recv_with_timeout` would
            // re-poll the routine; output waker on OutputQueue so
            // push() wakes the Output phase to drain.
            input: InputQueue::new(wakers.work),
            output: OutputQueue::new(wakers.output),
        }
    }
}

impl crate::Send<usize> for PollDouble {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.input.producer.push(Input::Data(message));
        Ok(())
    }
}

impl crate::Next<usize> for PollDouble {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl crate::Flush for PollDouble {
    fn flush(&mut self) -> Result<(), Error> {
        self.input.producer.push(Input::Flush);
        Ok(())
    }
}

impl crate::Poll for PollDouble {
    fn poll(&mut self, waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        let mut cx = core::task::Context::from_waker(&waker.sync);
        loop {
            match std::pin::Pin::new(&mut self.input.consumer.recv()).poll(&mut cx) {
                core::task::Poll::Ready(Ok(Input::Data(value))) => {
                    self.output.producer.push(value * 2);
                }
                core::task::Poll::Ready(Ok(Input::Flush)) => {
                    self.input.producer.reset().ok();
                    return Ok(core::task::Poll::Ready(()));
                }
                core::task::Poll::Ready(Err(_)) => {
                    return Ok(core::task::Poll::Ready(()));
                }
                core::task::Poll::Pending => {
                    return Ok(core::task::Poll::Pending);
                }
            }
        }
    }
}

impl Name for PollDouble {}
impl poll::LineRoutine<usize, usize> for PollDouble {}

/// Accumulates input values into a shared Vec. Used to verify sink nodes
/// actually receive and process data.
struct PollAccumulator {
    collected: Arc<Mutex<Vec<usize>>>,
    flushed: bool,
}

impl PollAccumulator {
    fn new(collected: Arc<Mutex<Vec<usize>>>) -> Self {
        Self {
            collected,
            flushed: false,
        }
    }
}

impl crate::Send<usize> for PollAccumulator {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.collected.lock().unwrap().push(message);
        Ok(())
    }
}

impl crate::Next<usize> for PollAccumulator {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl crate::Flush for PollAccumulator {
    fn flush(&mut self) -> Result<(), Error> {
        self.flushed = true;
        Ok(())
    }
}

impl crate::Poll for PollAccumulator {
    fn poll(&mut self, _waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        if self.flushed {
            self.flushed = false;
            return Ok(core::task::Poll::Ready(()));
        }
        Ok(core::task::Poll::Pending)
    }
}

impl Name for PollAccumulator {}
impl crate::LineRoutine<usize, usize> for PollAccumulator {}
impl poll::LineRoutine<usize, usize> for PollAccumulator {}

/// Sync Double — used for the sync part of the graph.
struct Double {
    output: VecDeque<usize>,
}

impl Double {
    fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

impl crate::Send<usize> for Double {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.push_back(message * 2);
        Ok(())
    }
}

impl crate::Next<usize> for Double {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop_front())
    }
}

impl crate::Flush for Double {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Name for Double {}
impl crate::LineRoutine<usize, usize> for Double {}

#[derive(Debug)]
struct IoThread;
impl ThreadId for IoThread {}

/// Sync writer → Node<Sync, Sync> (terminal) → sync output.
/// Data only appears in output after poll() processes it.
#[test]
fn sync_to_async_terminal_to_sync() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let mut node = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>()
        .output::<crate::poll::Sync>();

    make_push(&mut writer_node, &node)?;

    let output = Receiver::new();
    make_push(&mut node, &output)?;

    async_thread.add(node);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // 5 → sync Double → 10 → async PollDouble (send queues, poll doubles) → 20
        writer.push(Message::Data(5))?;
        assert_eq!(output.read_front()?, Message::Data(20));

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Node<Sync, Async> → Node<Async, Sync> via merge.
/// Both async nodes use poll() to process data.
#[test]
fn async_chain_with_local_edges() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let parent = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_node, &parent)?;

    let mut child = async_thread
        .line(|w| PollDouble::new(w))
        .parent(parent)
        .output::<crate::poll::Sync>();

    let output = Receiver::new();
    make_push(&mut child, &output)?;

    async_thread.add(child);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // 3 → sync Double → 6 → async PollDouble → 12 → async PollDouble → 24
        writer.push(Message::Data(3))?;
        assert_eq!(output.read_front()?, Message::Data(24));

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Five async nodes chained via merge: parent → linked → linked → linked → child.
#[test]
fn long_async_chain() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let a = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_node, &a)?;

    let b = async_thread.line(|w| PollDouble::new(w)).parent(a);
    let c = async_thread.line(|w| PollDouble::new(w)).parent(b);
    let d = async_thread.line(|w| PollDouble::new(w)).parent(c);
    let mut e = async_thread
        .line(|w| PollDouble::new(w))
        .parent(d)
        .output::<crate::poll::Sync>();

    let output = Receiver::new();
    make_push(&mut e, &output)?;

    async_thread.add(e);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // 1 → sync Double → 2
        //   → a PollDouble → 4
        //   → b PollDouble → 8
        //   → c PollDouble → 16
        //   → d PollDouble → 32
        //   → e PollDouble → 64
        writer.push(Message::Data(1))?;
        assert_eq!(output.read_front()?, Message::Data(64));

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Fan-out via cross-thread sync bridge (Mutex) between Node<Sync, Sync> terminals.
///
/// ```text
///                  ┌→ node_b → output_b
/// writer → node_a ─┤
///                  └→ node_c → output_c
/// ```
///
/// Uses SyncBridge (Mutex) for push connections between async nodes.
/// For zero-Mutex local edges, use merge — but that enforces DAG.
/// Push connections allow fan-out at the cost of Mutex.
#[test]
fn async_fan_out_via_sync_bridge() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let mut node_a = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>()
        .output::<crate::poll::Sync>();
    let mut node_b = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>()
        .output::<crate::poll::Sync>();
    let mut node_c = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>()
        .output::<crate::poll::Sync>();

    make_push(&mut writer_node, &node_a)?;
    make_push(&mut node_a, &node_b)?;
    make_push(&mut node_a, &node_c)?;

    let output_b = Receiver::new();
    let output_c = Receiver::new();
    make_push(&mut node_b, &output_b)?;
    make_push(&mut node_c, &output_c)?;

    async_thread.add(node_a);
    async_thread.add(node_b);
    async_thread.add(node_c);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

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
    let mut writer_a_node = crate::work::make_line(Double::new());
    let mut writer_a = crate::work::Writer::<usize>::of(&writer_a_node)?;

    let mut writer_b_node = crate::work::make_line(Double::new());
    let mut writer_b = crate::work::Writer::<usize>::of(&writer_b_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let parent_a = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_a_node, &parent_a)?;

    let parent_b = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_b_node, &parent_b)?;

    let mut child = async_thread
        .line(|w| PollDouble::new(w))
        .parent(parent_a)
        .parent(parent_b)
        .output::<crate::poll::Sync>();

    let output = Receiver::new();
    make_push(&mut child, &output)?;

    async_thread.add(child);

    // Independent blocking writers need separate sync threads —
    // work() calls read_front() which blocks, preventing other
    // writers on the same thread from being serviced.
    let mut sync_a = ThreadStream::<'_, crate::DefaultThread>::new();
    let mut sync_b = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_a_node, &mut sync_a)?;
    make_work(writer_b_node, &mut sync_b)?;

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
    let mut writer_a_node = crate::work::make_line(Double::new());
    let mut writer_a = crate::work::Writer::<usize>::of(&writer_a_node)?;

    let mut writer_b_node = crate::work::make_line(Double::new());
    let mut writer_b = crate::work::Writer::<usize>::of(&writer_b_node)?;

    let mut writer_c_node = crate::work::make_line(Double::new());
    let mut writer_c = crate::work::Writer::<usize>::of(&writer_c_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let parent_a = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_a_node, &parent_a)?;

    let parent_b = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_b_node, &parent_b)?;

    let parent_c = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_c_node, &parent_c)?;

    // All three parents merged into one linked node
    let linked = async_thread
        .line(|w| PollDouble::new(w))
        .parent(parent_a)
        .parent(parent_b)
        .parent(parent_c);

    let mut child = async_thread
        .line(|w| PollDouble::new(w))
        .parent(linked)
        .output::<crate::poll::Sync>();

    let output = Receiver::new();
    make_push(&mut child, &output)?;

    async_thread.add(child);

    // Independent blocking writers need separate sync threads.
    let mut sync_a = ThreadStream::<'_, crate::DefaultThread>::new();
    let mut sync_b = ThreadStream::<'_, crate::DefaultThread>::new();
    let mut sync_c = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_a_node, &mut sync_a)?;
    make_work(writer_b_node, &mut sync_b)?;
    make_work(writer_c_node, &mut sync_c)?;

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

// ============================================================
// Tests using unified Node with thread.line()
// ============================================================

/// Terminal via typed: Node<Sync, Sync>.
#[test]
fn node_terminal_via_typed() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let mut node = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>()
        .output::<crate::poll::Sync>();

    make_push(&mut writer_node, &node)?;

    let output = Receiver::new();
    make_push(&mut node, &output)?;

    async_thread.add(node);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // 5 → Double → 10 → PollDouble → 20
        writer.push(Message::Data(5))?;
        assert_eq!(output.read_front()?, Message::Data(20));

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Parent→child via typed + parent.
#[test]
fn node_parent_child_via_typed_and_parent() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let parent = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_node, &parent)?;

    let mut child = async_thread
        .line(|w| PollDouble::new(w))
        .parent(parent)
        .output::<crate::poll::Sync>();

    let output = Receiver::new();
    make_push(&mut child, &output)?;

    async_thread.add(child);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // 3 → Double → 6 → PollDouble → 12 → PollDouble → 24
        writer.push(Message::Data(3))?;
        assert_eq!(output.read_front()?, Message::Data(24));

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Sink: Deferred output, data discarded via Null.
#[test]
fn node_sink_deferred_output() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let parent = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_node, &parent)?;

    let sink = async_thread.line(|w| PollDouble::new(w)).parent(parent);
    async_thread.add(sink);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

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
    let mut writer_a_node = crate::work::make_line(Double::new());
    let mut writer_a = crate::work::Writer::<usize>::of(&writer_a_node)?;

    let mut writer_b_node = crate::work::make_line(Double::new());
    let mut writer_b = crate::work::Writer::<usize>::of(&writer_b_node)?;

    let mut writer_c_node = crate::work::make_line(Double::new());
    let mut writer_c = crate::work::Writer::<usize>::of(&writer_c_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let parent_a = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_a_node, &parent_a)?;

    let parent_b = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_b_node, &parent_b)?;

    let parent_c = async_thread
        .line(|w| PollDouble::new(w))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_c_node, &parent_c)?;

    let collected_1 = Arc::new(Mutex::new(Vec::new()));
    let collected_2 = Arc::new(Mutex::new(Vec::new()));

    // Sink 1: merges a + b, accumulates doubled values
    // writer_a: 1 → Double → 2 → PollDouble → 4 → sink_1 accumulates 4
    // writer_b: 2 → Double → 4 → PollDouble → 8 → sink_1 accumulates 8
    let sink_1 = async_thread
        .line({
            let c = collected_1.clone();
            move |_w| PollAccumulator::new(c)
        })
        .parent(parent_a)
        .parent(parent_b);
    async_thread.add(sink_1);

    // Sink 2: owns c, accumulates doubled values
    // writer_c: 3 → Double → 6 → PollDouble → 12 → sink_2 accumulates 12
    let sink_2 = async_thread
        .line({
            let c = collected_2.clone();
            move |_w| PollAccumulator::new(c)
        })
        .parent(parent_c);
    async_thread.add(sink_2);

    let mut sync_a = ThreadStream::<'_, crate::DefaultThread>::new();
    let mut sync_b = ThreadStream::<'_, crate::DefaultThread>::new();
    let mut sync_c = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_a_node, &mut sync_a)?;
    make_work(writer_b_node, &mut sync_b)?;
    make_work(writer_c_node, &mut sync_c)?;

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

// ============================================================
// Half-close flush test
// ============================================================

/// Simulates an I/O routine with a half-close handshake on flush.
/// Uses waker-aware Queue to wake Work on Send/Flush.
///
/// - send() pushes data to queue (wakes Work)
/// - flush() pushes flush marker to queue (wakes Work)
/// - poll() drains queue, doubles data, simulates multi-cycle handshake on flush
struct HalfCloseRoutine {
    input: InputQueue<usize>,
    output: OutputQueue<usize>,
    flush_cycles: usize,
    flush_cycles_remaining: usize,
    flush_count: Arc<Mutex<usize>>,
}

impl HalfCloseRoutine {
    fn new(wakers: LineWakers, flush_cycles: usize, flush_count: Arc<Mutex<usize>>) -> Self {
        Self {
            input: InputQueue::new(wakers.work),
            output: OutputQueue::new(wakers.output),
            flush_cycles,
            flush_cycles_remaining: 0,
            flush_count,
        }
    }
}

impl crate::Send<usize> for HalfCloseRoutine {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.input.producer.push(Input::Data(message));
        Ok(())
    }
}

impl crate::Next<usize> for HalfCloseRoutine {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl crate::Flush for HalfCloseRoutine {
    fn flush(&mut self) -> Result<(), Error> {
        self.input.producer.push(Input::Flush);
        Ok(())
    }
}

impl crate::Poll for HalfCloseRoutine {
    fn poll(&mut self, waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        let mut cx = core::task::Context::from_waker(&waker.sync);
        // If mid-handshake, count down
        if self.flush_cycles_remaining > 0 {
            self.flush_cycles_remaining -= 1;
            if self.flush_cycles_remaining > 0 {
                waker.sync.wake_by_ref();
                return Ok(core::task::Poll::Pending);
            }
            *self.flush_count.lock().unwrap() += 1;
            return Ok(core::task::Poll::Ready(()));
        }

        // Drain queue until flush or empty
        loop {
            match std::pin::Pin::new(&mut self.input.consumer.recv()).poll(&mut cx) {
                core::task::Poll::Ready(Ok(Input::Data(val))) => {
                    self.output.producer.push(val * 2);
                }
                core::task::Poll::Ready(Ok(Input::Flush)) => {
                    self.input.producer.reset().ok();
                    self.flush_cycles_remaining = self.flush_cycles;
                    waker.sync.wake_by_ref();
                    return Ok(core::task::Poll::Pending);
                }
                core::task::Poll::Ready(Err(_)) => {
                    return Ok(core::task::Poll::Ready(()));
                }
                core::task::Poll::Pending => {
                    return Ok(core::task::Poll::Pending);
                }
            }
        }
    }
}

impl Name for HalfCloseRoutine {}
impl poll::LineRoutine<usize, usize> for HalfCloseRoutine {}

/// Flush signal is held until the routine's poll() returns Ready.
///
/// HalfCloseRoutine takes 3 poll cycles to complete the "handshake".
/// The Flush signal only reaches the output after those 3 cycles.
/// Data sent before the flush is doubled and forwarded.
#[test]
fn flush_waits_for_routine_ready() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let flush_count = Arc::new(Mutex::new(0));
    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    // HalfCloseRoutine needs 3 poll cycles to complete flush handshake
    let parent = async_thread
        .line({
            let fc = flush_count.clone();
            move |w| HalfCloseRoutine::new(w, 3, fc)
        })
        .input::<crate::poll::Sync>();
    make_push(&mut writer_node, &parent)?;

    // Child collects output to verify flush ordering
    let mut child = async_thread
        .line(|w| PollDouble::new(w))
        .parent(parent)
        .output::<crate::poll::Sync>();

    let output = Receiver::new();
    make_push(&mut child, &output)?;

    async_thread.add(child);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // Send data then flush
        // 5 → Double → 10 → HalfCloseRoutine.send(10) → pending=[10]
        writer.push(Message::Data(5))?;

        // Flush: HalfCloseRoutine.flush() → output=[20], starts 3-cycle handshake
        // After 3 poll cycles: Ready → Flush signal forwarded → child receives it
        writer.push(Message::Flush("segment-1".into()))?;

        // Data(20) arrives at child (PollDouble) → 40
        assert_eq!(output.read_front()?, Message::Data(40));

        // Flush signal arrives at child after handshake completes
        assert_eq!(output.read_front()?, Message::Flush("segment-1".into()));

        // Verify handshake completed exactly once
        assert_eq!(*flush_count.lock().unwrap(), 1);

        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Accumulates data until flush, then outputs the sum. Simulates a
/// routine that batches work and only emits results on flush boundaries.
/// Takes `flush_cycles` poll cycles to complete each flush (I/O simulation).
struct BatchRoutine {
    accumulator: usize,
    output: OutputQueue<usize>,
    flush_requested: bool,
    flush_cycles: usize,
    flush_cycles_remaining: usize,
}

impl BatchRoutine {
    fn new(wakers: LineWakers, flush_cycles: usize) -> Self {
        Self {
            accumulator: 0,
            output: OutputQueue::new(wakers.output),
            flush_requested: false,
            flush_cycles,
            flush_cycles_remaining: 0,
        }
    }
}

impl crate::Send<usize> for BatchRoutine {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.accumulator += message;
        Ok(())
    }
}

impl crate::Next<usize> for BatchRoutine {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl crate::Flush for BatchRoutine {
    fn flush(&mut self) -> Result<(), Error> {
        if self.accumulator > 0 {
            self.output.producer.push(self.accumulator);
        }
        self.accumulator = 0;
        self.flush_requested = true;
        self.flush_cycles_remaining = self.flush_cycles;
        Ok(())
    }
}

impl crate::Poll for BatchRoutine {
    fn poll(&mut self, waker: &mut waker::Waker) -> Result<core::task::Poll<()>, Error> {
        if self.flush_requested {
            if self.flush_cycles_remaining == 0 {
                self.flush_requested = false;
                return Ok(core::task::Poll::Ready(()));
            }
            self.flush_cycles_remaining -= 1;
            waker.sync.wake_by_ref();
        }
        Ok(core::task::Poll::Pending)
    }
}

impl Name for BatchRoutine {}
impl poll::LineRoutine<usize, usize> for BatchRoutine {}

/// Multiple flushes then close. BatchRoutine accumulates data between
/// flushes and emits the sum on each flush boundary.
///
/// ```text
/// writer → Double → BatchRoutine(2 cycles) → PollDouble → output
/// ```
///
/// Sequence:
///   Data(1), Data(2), Flush("s1")  → batch emits 6 (2+4), PollDouble → 12
///   Data(3), Flush("s2")           → batch emits 6, PollDouble → 12
///   Close                          → empty flush, close propagates
#[test]
fn multi_flush_then_close() -> Result<(), Error> {
    let mut writer_node = crate::work::make_line(Double::new());
    let mut writer = crate::work::Writer::<usize>::of(&writer_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let parent = async_thread
        .line(|w| BatchRoutine::new(w, 2))
        .input::<crate::poll::Sync>();
    make_push(&mut writer_node, &parent)?;

    let mut child = async_thread
        .line(|w| PollDouble::new(w))
        .parent(parent)
        .output::<crate::poll::Sync>();

    let output = Receiver::new();
    make_push(&mut child, &output)?;

    async_thread.add(child);

    let mut sync_thread = ThreadStream::<'_, crate::DefaultThread>::new();
    make_work(writer_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        // Segment 1: Data(1), Data(2), Flush
        // Double: 1→2, 2→4. Batch accumulates 2+4=6.
        // Flush: emits 6, 2-cycle handshake, Flush("s1") forwarded.
        // PollDouble: 6→12
        writer.push(Message::Data(1))?;
        writer.push(Message::Data(2))?;
        writer.push(Message::Flush("s1".into()))?;

        assert_eq!(output.read_front()?, Message::Data(12));
        assert_eq!(output.read_front()?, Message::Flush("s1".into()));

        // Segment 2: Data(3), Flush
        // Double: 3→6. Batch accumulates 6.
        // Flush: emits 6, 2-cycle handshake, Flush("s2") forwarded.
        // PollDouble: 6→12
        writer.push(Message::Data(3))?;
        writer.push(Message::Flush("s2".into()))?;

        assert_eq!(output.read_front()?, Message::Data(12));
        assert_eq!(output.read_front()?, Message::Flush("s2".into()));

        // Close: flush() called (accumulator=0, nothing emitted).
        // poll() returns Ready → close propagates.
        writer.close()?;
        let errors = handle.join().errors();
        assert!(errors.is_empty());
        Ok(())
    })
}

/// Regression: `recv_with_timeout` must actually fire at its deadline.
///
/// Internally, `RecvTimeoutFut::poll` calls `schedule_at` on the local
/// waker stored in `InputQueue`. That waker MUST be bound to the work
/// phase (the one that polls the future) — if it's bound to a different
/// phase (e.g. output), the deadline fires the wrong phase and the
/// future never re-polls.
///
/// Routine: try `recv_with_timeout(10ms)` once. On `None` (timeout
/// fired), push 999 + drain to Flush. On `Some(_)` (input arrived
/// first — which means the test pushed Flush before the timeout
/// fired, indicating the waker is mis-routed), push 0.
///
/// Test: don't push anything for 100ms (plenty of slack for the 10ms
/// timeout). Then close the writer to terminate. Assert first output
/// is 999. With the bug, it's 0.
#[test]
fn recv_with_timeout_fires_on_deadline() -> Result<(), Error> {
    use crate::poll::future::line::FutureRoutine;
    use crate::poll::future::queue::{InputConsumer, OutputProducer};
    use crate::writer::push::Writer;
    use std::time::Duration;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let mut node = async_thread
        .line(FutureRoutine::factory(
            |input: InputConsumer<usize>, output: OutputProducer<usize>| {
                Box::pin(async move {
                    match input.recv_with_timeout(Duration::from_millis(10)).await? {
                        None => {
                            output.push(999);
                            // Drain until Flush so the input queue
                            // closes cleanly before we return.
                            loop {
                                match input.recv().await? {
                                    Input::Data(_) => continue,
                                    Input::Flush => break,
                                }
                            }
                        }
                        Some(_) => output.push(0),
                    }
                    Ok(())
                })
            },
        ))
        .input::<crate::poll::Sync>()
        .output::<crate::poll::Sync>();

    let mut writer = Writer::<usize>::of::<_, crate::marker::Unary>(&node).unwrap();
    let output = Receiver::new();
    make_push(&mut node, &output)?;
    async_thread.add(node);

    std::thread::scope(|s| -> Result<(), Error> {
        let handle = async_thread.start(s);

        // Sleep well past the 10ms timeout. If the waker is wired
        // correctly, recv_with_timeout returns None during this window
        // and the routine emits 999.
        std::thread::sleep(Duration::from_millis(100));
        writer.close()?;

        match output.read_front() {
            Ok(Message::Data(999)) => {}
            Ok(other) => panic!(
                "recv_with_timeout did not fire — got {:?} instead of 999. \
                 schedule_at is likely routing to the wrong phase.",
                other
            ),
            Err(e) => panic!(
                "no output received: {} — recv_with_timeout never resolved.",
                e
            ),
        }

        match handle.join() {
            crate::thread::Join::Ok => Ok(()),
            other => panic!("async thread did not exit cleanly: {:?}", other),
        }
    })
}

// ============================================================
// Tests for direct sink output (`.sink(...)`)
// ============================================================

/// Deliberately not `Clone` — a `Sync` output could not carry it.
struct Moved(&'static str);

/// A caller-provided sink: forwards moved data and signals its close, so a
/// test blocks on the channels instead of polling shared state.
struct Collect {
    items: std::sync::mpsc::Sender<Moved>,
    closed: std::sync::mpsc::Sender<()>,
}

impl Connection for Collect {}

impl Pushable for Collect {
    type DataType = Moved;
    type SignalType = Trackable<&'static str>;

    fn push(&mut self, msg: Message<Moved, Self::SignalType>) -> Result<(), Error> {
        if let Message::Data(data) = msg {
            self.items.send(data).unwrap();
        }
        Ok(())
    }
}

impl Closeable for Collect {
    fn close(&mut self) -> Result<(), Error> {
        self.closed.send(()).unwrap();
        Ok(())
    }
}

/// A direct sink is the node's single consumer: output moves into it — no
/// `Clone` on the data type — and node teardown closes it.
#[test]
fn node_direct_sink_moves_output_and_closes() -> Result<(), Error> {
    let (items, received) = std::sync::mpsc::channel();
    let (closed, close_signal) = std::sync::mpsc::channel();

    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let node = async_thread
        .line(FutureRoutine::factory(
            |input: InputConsumer<Moved>, output: OutputProducer<Moved>| {
                Box::pin(async move {
                    loop {
                        match input.recv().await? {
                            Input::Data(data) => output.push(data),
                            Input::Flush => return Ok(()),
                        }
                    }
                })
            },
        ))
        .input::<crate::poll::Sync>()
        .sink(Collect { items, closed });
    let mut writer = crate::work::Writer::<Moved>::new(&node)?;
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
        assert!(
            matches!(handle.join(), crate::thread::Join::Ok),
            "clean node exit",
        );
        close_signal
            .recv_timeout(timeout)
            .expect("node teardown closes the sink");
        Ok(())
    })
}
