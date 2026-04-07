//! Async thread that runs [Pollable] nodes driven by wakers.

use crate::connect::poll::graph::GraphBuilder;
use crate::connect::poll::queue::{Consumer, PollQueue};
use crate::connect::poll::runtime::{self, Runtime};
use crate::connect::poll::wakers::WakerAllocator;
use crate::error::{Error, ErrorKind};
use crate::node::biunion::poll::builder::node::Node as BiunionNode;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::node::line::poll::builder::node::Node;
use crate::node::line::poll::routine::LineRoutine;
use crate::{Origin, ThreadId, fatal};
use std::thread::{JoinHandle, spawn};

/// An idle async thread. Add builders via [Thread::add], then
/// call [Thread::start] to spawn the OS thread.
pub struct Thread<ThreadIdType: ThreadId + 'static> {
    builders: Vec<Box<dyn GraphBuilder<ThreadIdType>>>,
    waker_allocator: WakerAllocator,
    queue: PollQueue,
}

/// Handle to a running async thread.
pub struct ThreadHandle {
    thread: JoinHandle<Result<(), Error>>,
}

impl<ThreadIdType: ThreadId + 'static> Thread<ThreadIdType> {
    pub fn new() -> Self {
        let queue = PollQueue::new();
        let producer = queue.producer();
        Self {
            builders: Vec::new(),
            waker_allocator: WakerAllocator::new(producer),
            queue,
        }
    }

    /// Create a node with deferred edge kinds.
    ///
    /// Resolve input and output via wiring methods:
    /// - `.input::<Sync>()` → resolve input to Sync
    /// - `.output::<Sync>()` → resolve output to Sync
    /// - `.parent(node)` → Async input (adds parent)
    /// - consumed by `.parent()` → Deferred output (AsyncParent)
    pub fn line<InType, OutType, SignalType, FactoryType>(
        &mut self,
        factory: FactoryType,
    ) -> Node<
        '_,
        crate::poll::Deferred,
        crate::poll::Deferred,
        InType,
        OutType,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
    where
        SignalType: Origin,
        FactoryType: crate::node::line::poll::factory::LineRoutineFactory,
        FactoryType::Routine: LineRoutine<InType, OutType>,
    {
        Node::deferred(factory, &mut self.waker_allocator)
    }

    /// Create a biunion node with two deferred inputs and deferred output.
    ///
    /// Resolve inputs and output via wiring methods:
    /// - `.input::<Left, Sync>()` / `.input::<Right, Sync>()` → Sync input
    /// - `.parent::<Left>(node)` / `.parent::<Right>(node)` → Async input
    /// - `.output::<Sync>()` → Sync output
    pub fn biunion<Left, Right, Out, SignalType, FactoryType>(
        &mut self,
        factory: FactoryType,
    ) -> BiunionNode<
        crate::node::biunion::poll::builder::node::Allocating<'_>,
        crate::poll::Deferred,
        crate::poll::Deferred,
        crate::poll::Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
    where
        SignalType: Origin,
        FactoryType: BiunionRoutineFactory,
        FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    {
        BiunionNode::deferred(factory, &mut self.waker_allocator)
    }

    /// Add a node to the thread. It will be built when the thread starts.
    pub fn add(&mut self, builder: impl GraphBuilder<ThreadIdType> + 'static) {
        self.builders.push(Box::new(builder));
    }

    /// Start the async thread. Consumes self, returns a handle.
    pub fn start(self) -> ThreadHandle {
        let builders = self.builders;
        let waker_allocator = self.waker_allocator;
        let queue = self.queue;

        ThreadHandle {
            thread: spawn(move || {
                let (mut runtime, consumer) = match prepare(builders, waker_allocator, queue) {
                    Ok(r) => r,
                    Err(e) => {
                        #[cfg(not(feature = "silent"))]
                        eprintln!("Thread prepare: {}", e);
                        return Err(e);
                    }
                };
                poll_loop(&mut runtime, &consumer)
            }),
        }
    }
}

/// Build the runtime and return it with the consumer for the poll loop.
fn prepare<ThreadIdType: ThreadId>(
    builders: Vec<Box<dyn GraphBuilder<ThreadIdType>>>,
    waker_allocator: WakerAllocator,
    queue: PollQueue,
) -> Result<(Runtime<ThreadIdType>, Consumer), Error> {
    let (consumer, local_producer) = queue.local();
    let mut allocator = waker_allocator.local::<ThreadIdType>(local_producer);
    let mut all_nodes = Vec::new();

    for builder in builders {
        let graph = builder.build(allocator)?;
        allocator = graph.allocator;
        all_nodes.extend(graph.nodes);
    }

    let runtime = allocator.build(all_nodes)?;
    Ok((runtime, consumer))
}

impl ThreadHandle {
    /// Join the async thread.
    pub fn join(self) -> Result<Option<Error>, Error> {
        match self.thread.join() {
            Ok(Ok(())) => Ok(None),
            Ok(Err(e)) => Ok(Some(e)),
            Err(panic_err) => Err(fatal!("Thread panicked: {:?}", panic_err)),
        }
    }
}

/// Waker-driven poll loop. Blocks on ready queue, polls only woken nodes.
/// Runs until all nodes are closed.
fn poll_loop<ThreadIdType: ThreadId>(
    runtime: &mut Runtime<ThreadIdType>,
    consumer: &Consumer,
) -> Result<(), Error> {
    let mut closed = vec![false; runtime.nodes.len()];
    let mut closed_count = 0;

    while closed_count < runtime.nodes.len() {
        let node_id = match consumer.next() {
            Ok(id) => id,
            Err(e) => {
                #[cfg(not(feature = "silent"))]
                eprintln!("Thread dequeue error: {}", e);
                return Err(e);
            }
        };

        if closed[node_id] {
            continue;
        }

        let node = &mut runtime.nodes[node_id];
        let runtime::Node { pollable, waker } = node;

        match pollable.poll(waker) {
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
                eprintln!("Thread node {} error: {}", node_id, e);
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
    use crate::node::line::poll::routine::tests::MockLine;
    use crate::{Closeable, Message, SyncEdge, make_push};
    use std::sync::Arc;

    #[derive(Debug)]
    struct IoThread;
    impl ThreadId for IoThread {}

    #[test]
    fn async_thread_starts_and_stops() {
        let mut thread = Thread::<IoThread>::new();

        let node = thread
            .line(|w| MockLine::new(w))
            .input::<crate::poll::Sync>()
            .output::<crate::poll::Sync>();

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
        let mut thread = Thread::<IoThread>::new();

        let mut node = thread
            .line(|w| MockLine::new(w))
            .input::<crate::poll::Sync>()
            .output::<crate::poll::Sync>();

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
        let mut thread = Thread::<IoThread>::new();

        let parent = thread
            .line(|w| MockLine::new(w))
            .input::<crate::poll::Sync>();

        let mut input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
            Get::get(&parent).unwrap();

        let mut child = thread
            .line(|w| MockLine::new(w))
            .parent(parent)
            .output::<crate::poll::Sync>();

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
