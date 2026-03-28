//! Async thread that runs [Pollable] nodes driven by wakers.

use super::ready_queue::ReadyQueue;
use super::spawn::Spawnable;
use super::waker::NodeWaker;
use crate::connect::poll::edge::AsyncEdge;
use crate::error::{Error, ErrorKind};
use crate::node::line::poll::builder::child::ChildNode;
use crate::node::line::poll::builder::linked::LinkedNode;
use crate::node::line::poll::builder::parent::ParentNode;
use crate::node::line::poll::builder::terminal::TerminalNode;
use crate::{AsyncLineRoutine, Linkable, Origin, Pollable, ThreadId, fatal};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::thread::{JoinHandle, spawn};

/// An idle async thread. Add builders via [AsyncThread::add], then
/// call [AsyncThread::start] to spawn the OS thread.
pub struct AsyncThread<ThreadIdType: ThreadId + 'static> {
    builders: Vec<Box<dyn Spawnable<ThreadIdType>>>,
    ready_queue: Arc<ReadyQueue>,
    node_count: usize,
}

/// Handle to a running async thread.
pub struct AsyncThreadHandle {
    thread: JoinHandle<Result<(), Error>>,
}

impl<ThreadIdType: ThreadId + 'static> AsyncThread<ThreadIdType> {
    pub fn new() -> Self {
        Self {
            builders: Vec::new(),
            ready_queue: Arc::new(ReadyQueue::new()),
            node_count: 0,
        }
    }

    fn next_waker(&mut self) -> std::task::Waker {
        let node_id = self.node_count;
        self.node_count += 1;
        NodeWaker::new(node_id, self.ready_queue.clone())
    }

    /// Create a terminal node (sync in, sync out).
    /// Wire it with [crate::make_push]/[crate::graph::Get] then call [Self::add].
    pub fn terminal<In, Out, SignalType, RoutineType>(
        &mut self,
        routine: RoutineType,
    ) -> TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
    where
        In: Send + Sync + 'static,
        Out: Clone + Send + Sync + 'static,
        SignalType: Origin + Clone + Send + Sync + 'static,
        RoutineType: AsyncLineRoutine<In, Out>,
    {
        TerminalNode::new(routine, self.next_waker())
    }

    /// Create a parent node (sync in, async out).
    /// Pass it to [Self::child] or [Self::linked].
    pub fn parent<In, Out, SignalType, RoutineType>(
        &mut self,
        routine: RoutineType,
    ) -> ParentNode<In, Out, SignalType, ThreadIdType, RoutineType>
    where
        In: Send + Sync + 'static,
        Out: 'static,
        SignalType: Origin + Clone + Send + Sync + 'static,
        RoutineType: AsyncLineRoutine<In, Out>,
    {
        ParentNode::new(routine, self.next_waker())
    }

    /// Create a child node (async in, sync out) that owns a parent.
    /// Wire sync outputs, then call [Self::add].
    pub fn child<In, Out, SignalType, RoutineType>(
        &mut self,
        routine: RoutineType,
        parent: impl Linkable<
            crate::marker::Parent,
            Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
            Node = Box<dyn Pollable<ThreadId = ThreadIdType>>,
        > + 'static,
    ) -> ChildNode<In, Out, SignalType, ThreadIdType, RoutineType>
    where
        In: 'static,
        Out: Clone + Send + Sync + 'static,
        SignalType: Origin + Clone + Send + Sync + 'static,
        RoutineType: AsyncLineRoutine<In, Out>,
    {
        ChildNode::new(routine, self.next_waker(), Box::new(parent))
    }

    /// Create a linked node (async in, async out) that owns a parent.
    /// Pass the result to [Self::child] or another [Self::linked].
    pub fn linked<In, Out, SignalType, RoutineType>(
        &mut self,
        routine: RoutineType,
        parent: impl Linkable<
            crate::marker::Parent,
            Edge = Rc<RefCell<AsyncEdge<In, SignalType>>>,
            Node = Box<dyn Pollable<ThreadId = ThreadIdType>>,
        > + 'static,
    ) -> LinkedNode<In, Out, SignalType, ThreadIdType, RoutineType>
    where
        In: 'static,
        Out: 'static,
        SignalType: Origin + Clone + Send + Sync + 'static,
        RoutineType: AsyncLineRoutine<In, Out>,
    {
        LinkedNode::new(routine, self.next_waker(), Box::new(parent))
    }

    /// Add a node to the thread. It will be spawned when the thread starts.
    pub fn add(&mut self, builder: impl Spawnable<ThreadIdType> + 'static) {
        self.builders.push(Box::new(builder));
    }

    /// Start the async thread. Consumes self, returns a handle.
    pub fn start(self) -> AsyncThreadHandle {
        let ready_queue = self.ready_queue;
        let builders = self.builders;
        let expected_nodes = self.node_count;

        AsyncThreadHandle {
            thread: spawn(move || {
                let mut nodes: Vec<Box<dyn Pollable<ThreadId = ThreadIdType>>> = Vec::new();
                for builder in builders {
                    nodes.extend(builder.spawn());
                }

                if nodes.len() != expected_nodes {
                    return Err(fatal!(
                        "AsyncThread: expected {} nodes but got {}. \
                         Did you forget to add() a node?",
                        expected_nodes,
                        nodes.len()
                    ));
                }

                ready_queue.enqueue_all(0..nodes.len())?;

                poll_loop(&mut nodes, &ready_queue)
            }),
        }
    }
}

impl AsyncThreadHandle {
    /// Join the async thread.
    pub fn join(self) -> Result<Option<Error>, Error> {
        match self.thread.join() {
            Ok(Ok(())) => Ok(None),
            Ok(Err(e)) => Ok(Some(e)),
            Err(panic_err) => Err(fatal!("AsyncThread panicked: {:?}", panic_err)),
        }
    }
}

/// Waker-driven poll loop. Blocks on ready queue, polls only woken nodes.
fn poll_loop<ThreadIdType: ThreadId>(
    nodes: &mut [Box<dyn Pollable<ThreadId = ThreadIdType>>],
    ready_queue: &Arc<ReadyQueue>,
) -> Result<(), Error> {
    loop {
        let node_id = ready_queue.blocking_dequeue()?;

        if node_id >= nodes.len() {
            #[cfg(not(feature = "silent"))]
            eprintln!(
                "AsyncThread: invalid node_id {} (have {} nodes)",
                node_id,
                nodes.len()
            );
            continue;
        }

        let waker = NodeWaker::new(node_id, ready_queue.clone());
        let mut cx = Context::from_waker(&waker);

        match nodes[node_id].poll(&mut cx) {
            Ok(core::task::Poll::Pending) => {}
            Ok(core::task::Poll::Ready(())) => return Ok(()),
            Err(e) => match e.kind {
                ErrorKind::Closed => return Ok(()),
                _ => {
                    #[cfg(not(feature = "silent"))]
                    eprintln!("AsyncThread node {} error: {}", node_id, e);
                    return Err(e);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Get;
    use crate::node::line::routine::tests::AsyncMockLine;
    use crate::{Closeable, Message, SyncEdge, make_push};
    use std::sync::Arc;

    #[derive(Debug)]
    struct IoThread;
    impl ThreadId for IoThread {}

    #[test]
    fn async_thread_starts_and_stops() {
        let mut thread = AsyncThread::<IoThread>::new();

        let node = thread.terminal(AsyncMockLine::new());

        let mut input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
            Get::get(&node).unwrap();

        thread.add(node);

        let handle = thread.start();

        input.close().unwrap();

        let result = handle.join().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn async_thread_processes_data() {
        let mut thread = AsyncThread::<IoThread>::new();

        let mut node = thread.terminal(AsyncMockLine::new());

        let mut input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
            Get::get(&node).unwrap();

        let output = Arc::new(SyncEdge::new());
        make_push(&mut node, &output).unwrap();

        thread.add(node);
        let handle = thread.start();

        input.push(Message::Data(2)).unwrap();

        let result = output.read_front().unwrap();
        assert_eq!(result, Message::Data(4));

        input.close().unwrap();
        let result = handle.join().unwrap();
        assert!(result.is_none());
    }
}
