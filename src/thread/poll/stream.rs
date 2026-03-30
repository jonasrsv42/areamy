//! Async thread that runs [Pollable] nodes driven by wakers.

use super::ready_queue::ReadyQueue;
use super::spawn::{NodeId, Spawnable};
use super::waker::NodeWaker;
use crate::error::{Error, ErrorKind};
use crate::node::line::poll::builder::node::Node;
use std::task::Context;
use crate::{AsyncLineRoutine, Origin, Pollable, ThreadId, fatal};
use std::sync::Arc;
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

    fn next_node(&mut self) -> (NodeId, std::task::Waker) {
        let node_id = self.node_count;
        self.node_count += 1;
        let waker = NodeWaker::new(node_id, self.ready_queue.clone());
        (node_id, waker)
    }

    /// Create a node with deferred edge kinds.
    ///
    /// Resolve input and output via wiring methods:
    /// - `.typed::<OutEdge>()` → Sync input, explicit output kind
    /// - `.parent(node)` → Async input (adds parent)
    /// - `thread.add(node)` → Sync output (Spawnable)
    /// - consumed by `.parent()` → Async output (Linkable<Parent>)
    pub fn node<InType, OutType, SignalType, RoutineType>(
        &mut self,
        routine: RoutineType,
    ) -> Node<
        crate::poll::Deferred,
        crate::poll::Deferred,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        RoutineType,
    >
    where
        SignalType: Origin,
        RoutineType: AsyncLineRoutine<InType, OutType>,
    {
        let (node_id, waker) = self.next_node();
        Node::deferred(node_id, routine, waker)
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
                let mut nodes = build_nodes(builders, expected_nodes)?;
                ready_queue.enqueue_all(0..nodes.len())?;
                poll_loop(&mut nodes, &ready_queue)
            }),
        }
    }
}

/// Spawn all builders and place nodes at indices matching their waker IDs.
fn build_nodes<ThreadIdType: ThreadId>(
    builders: Vec<Box<dyn Spawnable<ThreadIdType>>>,
    expected: usize,
) -> Result<Vec<Box<dyn Pollable<ThreadId = ThreadIdType>>>, Error> {
    let mut pairs = Vec::new();
    for builder in builders {
        pairs.extend(builder.spawn());
    }

    if pairs.len() != expected {
        return Err(fatal!(
            "AsyncThread: expected {} nodes but got {}. Did you forget to add() a node?",
            expected,
            pairs.len()
        ));
    }

    let mut slots: Vec<Option<Box<dyn Pollable<ThreadId = ThreadIdType>>>> =
        (0..expected).map(|_| None).collect();

    for (node_id, node) in pairs {
        if node_id >= expected {
            return Err(fatal!("AsyncThread: node_id {} out of range", node_id));
        }
        if slots[node_id].is_some() {
            return Err(fatal!("AsyncThread: duplicate node_id {}", node_id));
        }
        slots[node_id] = Some(node);
    }

    slots
        .into_iter()
        .enumerate()
        .map(|(i, slot)| slot.ok_or_else(|| fatal!("AsyncThread: missing node at index {}", i)))
        .collect()
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
/// Runs until all nodes are closed.
fn poll_loop<ThreadIdType: ThreadId>(
    nodes: &mut [Box<dyn Pollable<ThreadId = ThreadIdType>>],
    ready_queue: &Arc<ReadyQueue>,
) -> Result<(), Error> {
    // Cache wakers — one per node, created once.
    let wakers: Vec<std::task::Waker> = (0..nodes.len())
        .map(|id| NodeWaker::new(id, ready_queue.clone()))
        .collect();

    let mut closed = vec![false; nodes.len()];
    let mut closed_count = 0;

    while closed_count < nodes.len() {
        let node_id = ready_queue.blocking_dequeue()?;

        if closed[node_id] {
            continue;
        }

        let mut cx = Context::from_waker(&wakers[node_id]);

        match nodes[node_id].poll(&mut cx) {
            Ok(core::task::Poll::Pending) => {}
            Ok(core::task::Poll::Ready(()))
            | Err(Error {
                kind: ErrorKind::Closed,
                ..
            }) => {
                closed[node_id] = true;
                closed_count += 1;
            }
            Err(e) => {
                #[cfg(not(feature = "silent"))]
                eprintln!("AsyncThread node {} error: {}", node_id, e);
                return Err(e);
            }
        }
    }

    Ok(())
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

        let node = thread
            .node(AsyncMockLine::new())
            .typed::<crate::poll::Sync>();

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

        let mut node = thread
            .node(AsyncMockLine::new())
            .typed::<crate::poll::Sync>();

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

    /// Close propagation: closing the source should propagate through
    /// the async chain and close the output edge.
    #[test]
    fn close_propagates_through_chain() {
        let mut thread = AsyncThread::<IoThread>::new();

        let parent = thread
            .node(AsyncMockLine::new())
            .typed::<crate::poll::Async>();

        let mut input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
            Get::get(&parent).unwrap();

        let mut child = thread
            .node(AsyncMockLine::new())
            .parent(parent)
            .typed::<crate::poll::Sync>();

        let output = Arc::new(SyncEdge::new());
        make_push(&mut child, &output).unwrap();

        thread.add(child);
        let handle = thread.start();

        // Send some data first
        input.push(Message::Data(1)).unwrap();
        assert_eq!(output.read_front().unwrap(), Message::Data(4));

        // Close input — should propagate: parent closes → child closes → output closes
        input.close().unwrap();

        // Output should be closed — reading should eventually return Closed
        let result = output.read_front();
        assert!(
            result.is_err(),
            "Expected Closed error after close propagation, got {:?}",
            result
        );

        let join_result = handle.join().unwrap();
        assert!(join_result.is_none());
    }
}
