//! Chains: sync and async nodes in a line, across the bridge and over local edges.

use crate::edge::sync::tests::wait_until;
use crate::error::Error;
use crate::poll;
use crate::sync::Receiver;
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::Join;
use crate::work;
use crate::{
    Closeable, DefaultThread, Message, Pushable, Pushes, Streams, ThreadBundle, Via, Writes,
};
use core::num::NonZeroUsize;

use super::mock::{Double, IoThread, PollDouble};

/// Sync writer → Node<Sync, Sync> (terminal) → sync output.
/// Data only appears in output after poll() processes it.
#[test]
fn sync_to_async_terminal_to_sync() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();
    let mut node = async_thread.line(PollDouble::new).input().output();

    writer_node.pushes(&node)?;

    let output = Receiver::new();
    node.pushes(&output)?;

    async_thread.add(node);

    let sync_thread = writer_node.stream::<DefaultThread>();

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

/// A node parked on a full edge leaves its poll thread running: a second node on the thread keeps
/// flowing, and the parked one finishes once its consumer drains.
#[test]
fn a_parked_node_leaves_its_thread_running() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    let mut async_thread = poll::Thread::<IoThread>::new();
    let mut stalled = async_thread.line(PollDouble::new).input().output();
    let mut flowing = async_thread.line(PollDouble::new).input().output();
    let mut stalled_writer = stalled.writer()?;
    let mut flowing_writer = flowing.writer()?;
    let full = Receiver::bounded(NonZeroUsize::MIN);
    let open = Receiver::new();
    stalled.pushes(&full)?;
    flowing.pushes(&open)?;
    async_thread.add(stalled);
    async_thread.add(flowing);

    // Moved in: a failing assertion unwinds through their drops, closing every edge so the poll
    // thread exits and the scope can surface the failure instead of hanging.
    std::thread::scope(move |s| -> Result<(), Error> {
        let handle = async_thread.start(s);
        for value in 1..=3 {
            stalled_writer.push(Message::Data(value))?;
        }
        // One delivered; the rest wait in the parked node, not in a blocked thread.
        wait_until(|| full.len().is_ok_and(|len| len == 1));
        for value in 1..=3 {
            flowing_writer.push(Message::Data(value))?;
        }
        wait_until(|| open.len().is_ok_and(|len| len == 3));
        for value in 1..=3 {
            assert_eq!(open.read_front()?, Message::Data(value * 2));
        }
        // Each pop wakes the parked node to deliver the next.
        for value in 1..=3 {
            wait_until(|| full.len().is_ok_and(|len| len == 1));
            assert_eq!(full.read_front()?, Message::Data(value * 2));
        }

        stalled_writer.close()?;
        flowing_writer.close()?;
        assert!(matches!(handle.join(), Join::Ok));
        Ok(())
    })
}

/// Node<Sync, Async> → Node<Async, Sync> via a local `via` edge.
/// Both async nodes use poll() to process data.
#[test]
fn async_chain_with_local_edges() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let parent = async_thread.line(PollDouble::new).input();
    writer_node.pushes(&parent)?;

    let mut child = parent.via(async_thread.line(PollDouble::new))?.output();

    let output = Receiver::new();
    child.pushes(&output)?;

    async_thread.add(child);

    let sync_thread = writer_node.stream::<DefaultThread>();

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

/// Five async nodes chained with `via`: a → b → c → d → e.
#[test]
fn long_async_chain() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let a = async_thread.line(PollDouble::new).input();
    writer_node.pushes(&a)?;

    let mut e = a
        .via(async_thread.line(PollDouble::new))?
        .via(async_thread.line(PollDouble::new))?
        .via(async_thread.line(PollDouble::new))?
        .via(async_thread.line(PollDouble::new))?
        .output();

    let output = Receiver::new();
    e.pushes(&output)?;

    async_thread.add(e);

    let sync_thread = writer_node.stream::<DefaultThread>();

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
