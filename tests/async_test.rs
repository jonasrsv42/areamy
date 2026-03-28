//! Integration tests for mixed sync + async graphs.

use areamy::error::Error;
use areamy::node::Name;
use areamy::{
    AsyncThread, Closeable, Message, Pushable, SyncEdge, ThreadBundle, ThreadId, ThreadStream,
    make_push, make_work,
};
use std::collections::VecDeque;
use std::sync::Arc;

/// Routine that queues input in send, then doubles it in poll.
/// This proves poll is actually being called by the async runtime —
/// without poll, no data reaches the output.
struct PollDouble {
    pending: VecDeque<usize>,
    output: VecDeque<usize>,
}

impl PollDouble {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            output: VecDeque::new(),
        }
    }
}

impl areamy::Send<usize> for PollDouble {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.pending.push_back(message);
        Ok(())
    }
}

impl areamy::Next<usize> for PollDouble {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop_front())
    }
}

impl areamy::Flush for PollDouble {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl areamy::Poll for PollDouble {
    fn poll(&mut self, _cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
        while let Some(value) = self.pending.pop_front() {
            self.output.push_back(value * 2);
        }
        Ok(core::task::Poll::Pending)
    }
}

impl Name for PollDouble {}
impl areamy::LineRoutine<usize, usize> for PollDouble {}
impl areamy::AsyncLineRoutine<usize, usize> for PollDouble {}

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

impl areamy::Send<usize> for Double {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.push_back(message * 2);
        Ok(())
    }
}

impl areamy::Next<usize> for Double {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop_front())
    }
}

impl areamy::Flush for Double {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Name for Double {}
impl areamy::LineRoutine<usize, usize> for Double {}

#[derive(Debug)]
struct IoThread;
impl ThreadId for IoThread {}

/// Sync source → async terminal (PollDouble) → sync output.
/// Data only appears in output after poll() processes it.
#[test]
fn sync_to_async_terminal_to_sync() -> Result<(), Error> {
    let mut source_node = areamy::work::make_line(Double::new());
    let mut source = areamy::work::Source::<usize>::of(&source_node)?;

    let mut async_thread = AsyncThread::<IoThread>::new();
    let mut async_node = async_thread.terminal(PollDouble::new());

    make_push(&mut source_node, &async_node)?;

    let output = Arc::new(SyncEdge::new());
    make_push(&mut async_node, &output)?;

    async_thread.add(async_node);

    let mut sync_thread = ThreadStream::<areamy::DefaultThread>::new();
    make_work(source_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    let handle = bundle.start();

    // 5 → sync Double → 10 → async PollDouble (send queues, poll doubles) → 20
    source.push(Message::Data(5))?;
    assert_eq!(output.read_front()?, Message::Data(20));

    source.close()?;
    let errors = handle.join()?;
    assert!(errors.is_empty());
    Ok(())
}

/// Sync source → async parent (PollDouble) → async child (PollDouble) → sync output.
/// Both async nodes use poll() to process data.
#[test]
fn async_chain_with_local_edges() -> Result<(), Error> {
    let mut source_node = areamy::work::make_line(Double::new());
    let mut source = areamy::work::Source::<usize>::of(&source_node)?;

    let mut async_thread = AsyncThread::<IoThread>::new();

    let parent = async_thread.parent(PollDouble::new());
    make_push(&mut source_node, &parent)?;

    let mut child = async_thread.child(PollDouble::new(), parent);

    let output = Arc::new(SyncEdge::new());
    make_push(&mut child, &output)?;

    async_thread.add(child);

    let mut sync_thread = ThreadStream::<areamy::DefaultThread>::new();
    make_work(source_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    let handle = bundle.start();

    // 3 → sync Double → 6 → async parent PollDouble → 12 → async child PollDouble → 24
    source.push(Message::Data(3))?;
    assert_eq!(output.read_front()?, Message::Data(24));

    source.close()?;
    let errors = handle.join()?;
    assert!(errors.is_empty());
    Ok(())
}

/// Sync source → parent → linked → linked → linked → child → sync output.
/// Five async nodes chained via local edges using linked().
#[test]
fn long_async_chain() -> Result<(), Error> {
    let mut source_node = areamy::work::make_line(Double::new());
    let mut source = areamy::work::Source::<usize>::of(&source_node)?;

    let mut async_thread = AsyncThread::<IoThread>::new();

    let a = async_thread.parent(PollDouble::new());
    make_push(&mut source_node, &a)?;

    let b = async_thread.linked(PollDouble::new(), a);
    let c = async_thread.linked(PollDouble::new(), b);
    let d = async_thread.linked(PollDouble::new(), c);
    let mut e = async_thread.child(PollDouble::new(), d);

    let output = Arc::new(SyncEdge::new());
    make_push(&mut e, &output)?;

    async_thread.add(e);

    let mut sync_thread = ThreadStream::<areamy::DefaultThread>::new();
    make_work(source_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    let handle = bundle.start();

    // 1 → sync Double → 2
    //   → a PollDouble → 4
    //   → b PollDouble → 8
    //   → c PollDouble → 16
    //   → d PollDouble → 32
    //   → e PollDouble → 64
    source.push(Message::Data(1))?;
    assert_eq!(output.read_front()?, Message::Data(64));

    source.close()?;
    let errors = handle.join()?;
    assert!(errors.is_empty());
    Ok(())
}
