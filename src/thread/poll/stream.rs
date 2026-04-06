//! Async thread that runs [Pollable] nodes driven by wakers.

use crate::connect::poll::graph::PollGraphBuilder;
use crate::connect::poll::queue::{Consumer, PollQueue};
use crate::connect::poll::runtime::{self, Runtime};
use crate::connect::poll::wakers::WakerAllocator;
use crate::error::{Error, ErrorKind};
use crate::node::line::poll::builder::node::Node;
use crate::{AsyncLineRoutine, Origin, ThreadId, fatal};
use std::thread::{JoinHandle, spawn};

/// An idle async thread. Add builders via [AsyncThread::add], then
/// call [AsyncThread::start] to spawn the OS thread.
pub struct AsyncThread<ThreadIdType: ThreadId + 'static> {
    builders: Vec<Box<dyn PollGraphBuilder<ThreadIdType>>>,
    waker_allocator: WakerAllocator,
    queue: PollQueue,
}

/// Handle to a running async thread.
pub struct AsyncThreadHandle {
    thread: JoinHandle<Result<(), Error>>,
}

impl<ThreadIdType: ThreadId + 'static> AsyncThread<ThreadIdType> {
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
    /// - `.typed::<OutEdge>()` → Sync input, explicit output kind
    /// - `.parent(node)` → Async input (adds parent)
    /// - `thread.add(node)` → Sync output (Spawnable)
    /// - consumed by `.parent()` → Async output (AsyncParent)
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
        FactoryType: crate::node::line::poll::factory::PollLineRoutineFactory,
        FactoryType::Routine: AsyncLineRoutine<InType, OutType>,
    {
        Node::deferred(factory, &mut self.waker_allocator)
    }

    /// Add a node to the thread. It will be built when the thread starts.
    pub fn add(&mut self, builder: impl PollGraphBuilder<ThreadIdType> + 'static) {
        self.builders.push(Box::new(builder));
    }

    /// Start the async thread. Consumes self, returns a handle.
    pub fn start(self) -> AsyncThreadHandle {
        let builders = self.builders;
        let waker_allocator = self.waker_allocator;
        let queue = self.queue;

        AsyncThreadHandle {
            thread: spawn(move || {
                let (mut runtime, consumer) = match prepare(builders, waker_allocator, queue) {
                    Ok(r) => r,
                    Err(e) => {
                        #[cfg(not(feature = "silent"))]
                        eprintln!("AsyncThread prepare: {}", e);
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
    builders: Vec<Box<dyn PollGraphBuilder<ThreadIdType>>>,
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
                eprintln!("AsyncThread dequeue error: {}", e);
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
            .line(|w| AsyncMockLine::new(w))
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
            .line(|w| AsyncMockLine::new(w))
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
            .line(|w| AsyncMockLine::new(w))
            .typed::<crate::poll::Async>();

        let mut input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
            Get::get(&parent).unwrap();

        let mut child = thread
            .line(|w| AsyncMockLine::new(w))
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
