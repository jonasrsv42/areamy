//! Async thread that runs [Pollable] nodes driven by wakers.

use super::runtime::ClosableRuntime;
use crate::connect::poll::graph::GraphBuilder;
use crate::connect::poll::queue::{Consumer, PollQueue};
use crate::connect::poll::runtime::Node as RuntimeNode;
use crate::connect::poll::wakers::WakerAllocator;
use crate::error::{Error, ErrorKind};
use crate::node::biunion::poll::builder::node::{Allocating, Node as BiunionNode};
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::node::line::poll::builder::node::Node;
use crate::node::line::poll::factory::LineRoutineFactory;
use crate::node::line::poll::routine::LineRoutine;
use crate::poll::Deferred;
use crate::thread::Join;
use crate::thread::callback::{self, OnDone, PanicGuard};
use crate::thread::done::Done;
use crate::{Origin, ThreadId, fatal};
use std::thread::{Scope, ScopedJoinHandle};

/// An idle async thread. Add builders via [Thread::add], then
/// call [Thread::start] to spawn the OS thread.
pub struct Thread<'params, ThreadIdType: ThreadId + 'static> {
    builders: Vec<Box<dyn GraphBuilder<'params, ThreadIdType> + 'params>>,
    waker_allocator: WakerAllocator,
    queue: PollQueue,
    on_done: Vec<OnDone>,
}

/// Handle to a running async thread.
pub struct ThreadHandle<'threads> {
    thread: ScopedJoinHandle<'threads, Result<(), Error>>,
}

impl<'params, ThreadIdType: ThreadId + 'static> Thread<'params, ThreadIdType> {
    pub fn new() -> Self {
        let queue = PollQueue::new();
        let producer = queue.producer();
        Self {
            builders: Vec::new(),
            waker_allocator: WakerAllocator::new(producer),
            queue,
            on_done: Vec::new(),
        }
    }

    /// Register a callback to fire when the thread exits. See
    /// [`ThreadStream::on_done`](crate::thread::ThreadStream::on_done)
    /// for the full contract — same semantics here.
    pub fn on_done<F>(&mut self, callback: F) -> &mut Self
    where
        F: FnOnce(&Done) + Send + 'static,
    {
        self.on_done.push(Box::new(callback));
        self
    }

    /// Mutable access to the thread's [`WakerAllocator`]. Lets callers
    /// pre-allocate sync waker slots before the thread starts — useful
    /// for nodes that need to be woken from external producers running
    /// on other threads.
    ///
    /// Each allocated slot ID must be bound to a [`Pollable`] via a
    /// custom [`GraphBuilder`] returning a [`GraphNode`] with that ID;
    /// otherwise the runtime will fail at build time with "slot N is
    /// empty."
    pub fn waker_allocator(&mut self) -> &mut WakerAllocator {
        &mut self.waker_allocator
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
    ) -> Node<'_, 'params, Deferred, Deferred, InType, OutType, SignalType, ThreadIdType, FactoryType>
    where
        SignalType: Origin,
        FactoryType: LineRoutineFactory<'params>,
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
        'params,
        Allocating<'_>,
        Deferred,
        Deferred,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
    where
        SignalType: Origin,
        FactoryType: BiunionRoutineFactory<'params>,
        FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    {
        BiunionNode::deferred(factory, &mut self.waker_allocator)
    }

    /// Add a node to the thread. It will be built when the thread starts.
    pub fn add(&mut self, builder: impl GraphBuilder<'params, ThreadIdType> + 'params) {
        self.builders.push(Box::new(builder));
    }

    /// Run the poll loop synchronously on the current thread, with
    /// PanicGuard + on_done callbacks. Used by [`start`](Self::start)
    /// (via `scope.spawn`) and the type-erase layer.
    pub(crate) fn run(self) -> Result<(), Error> {
        let Self {
            builders,
            waker_allocator,
            queue,
            on_done,
        } = self;

        let mut guard = PanicGuard::new(on_done);

        let result = match prepare(builders, waker_allocator, queue) {
            Ok((mut runtime, consumer)) => poll_loop(&mut runtime, &consumer),
            Err(e) => {
                #[cfg(not(feature = "silent"))]
                eprintln!("Thread prepare: {}", e);
                Err(e)
            }
        };

        let callbacks = guard.drain();
        let done = match &result {
            Ok(()) => Done::Close,
            Err(e) => Done::Error(e),
        };
        callback::fire(callbacks, &done);
        result
    }

    /// Start the async thread inside the provided `scope`. Consumes self,
    /// returns a handle bound to `'threads`.
    pub fn start<'threads>(
        self,
        scope: &'threads Scope<'threads, 'params>,
    ) -> ThreadHandle<'threads>
    where
        'params: 'threads,
    {
        let thread = scope.spawn(move || self.run());
        ThreadHandle { thread }
    }
}

/// Build the runtime and return it with the consumer for the poll loop.
fn prepare<'params, ThreadIdType: ThreadId + 'params>(
    builders: Vec<Box<dyn GraphBuilder<'params, ThreadIdType> + 'params>>,
    waker_allocator: WakerAllocator,
    queue: PollQueue,
) -> Result<(ClosableRuntime<'params, ThreadIdType>, Consumer), Error> {
    let (consumer, local_producer) = queue.local();
    let mut allocator = waker_allocator.local::<ThreadIdType>(local_producer);
    let mut all_nodes = Vec::new();

    for builder in builders {
        let graph = builder.build(allocator)?;
        allocator = graph.allocator;
        all_nodes.extend(graph.nodes);
    }

    let runtime = allocator.build(all_nodes)?;
    Ok((runtime.into(), consumer))
}

impl<'threads> ThreadHandle<'threads> {
    /// Join the async thread.
    pub fn join(self) -> Join {
        match self.thread.join() {
            Ok(Ok(())) => Join::Ok,
            Ok(Err(e)) => Join::Error(e),
            Err(panic_err) => Join::Panic(fatal!("Thread panicked: {:?}", panic_err)),
        }
    }
}

/// Waker-driven poll loop. Blocks on ready queue, polls only woken
/// nodes. A node returning `Ready` or `Closed` is taken out of its
/// slot here — its close-on-drop cascade fires immediately, and a
/// later wake for the same id is observable as `None`.
fn poll_loop<ThreadIdType: ThreadId>(
    runtime: &mut ClosableRuntime<ThreadIdType>,
    consumer: &Consumer,
) -> Result<(), Error> {
    let mut alive = runtime.nodes.len();

    while alive > 0 {
        let node_id = match consumer.next() {
            Ok(id) => id,
            Err(e) => {
                #[cfg(not(feature = "silent"))]
                eprintln!("Thread dequeue error: {}", e);
                return Err(e);
            }
        };

        let slot = &mut runtime.nodes[node_id];
        let Some(RuntimeNode { pollable, waker }) = slot.as_mut() else {
            // Benign: a cross-thread producer or sibling node can fire
            // this slot's waker after we eager-dropped the node on a
            // prior Ready/Closed. The wake event was already in flight
            // when the slot was set to None. Silently skip.
            continue;
        };

        match pollable.poll(waker) {
            Ok(core::task::Poll::Pending) => {}
            Ok(core::task::Poll::Ready(()))
            | Err(Error {
                kind: ErrorKind::Closed,
                ..
            }) => {
                // Drop the node here — its `Sender`/`Receiver` handles
                // drop with it and the cascade fires before we move on
                // to the next ready event.
                *slot = None;
                alive -= 1;
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
    use crate::connect::sync::Receiver;
    use crate::graph::Get;
    use crate::node::line::poll::routine::tests::MockLine;
    use crate::poll;
    use crate::{Closeable, Message, make_push};

    #[derive(Debug)]
    struct IoThread;
    impl ThreadId for IoThread {}

    type InputHandle =
        Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync>;

    #[test]
    fn async_thread_starts_and_stops() {
        let mut thread = Thread::<'_, IoThread>::new();

        let node = thread
            .line(|w| MockLine::new(w))
            .input::<poll::Sync>()
            .output::<poll::Sync>();

        let mut input: InputHandle = Get::get(&node).unwrap();

        thread.add(node);
        std::thread::scope(|s| {
            let handle = thread.start(s);
            input.close().unwrap();
            assert!(matches!(handle.join(), Join::Ok));
        });
    }

    #[test]
    fn async_thread_processes_data() {
        let mut thread = Thread::<'_, IoThread>::new();

        let mut node = thread
            .line(|w| MockLine::new(w))
            .input::<poll::Sync>()
            .output::<poll::Sync>();

        let mut input: InputHandle = Get::get(&node).unwrap();

        let output = Receiver::new();
        make_push(&mut node, &output).unwrap();

        thread.add(node);
        std::thread::scope(|s| {
            let handle = thread.start(s);
            input.push(Message::Data(2)).unwrap();
            assert_eq!(output.read_front().unwrap(), Message::Data(4));
            input.close().unwrap();
            assert!(matches!(handle.join(), Join::Ok));
        });
    }

    // ---- on_done callback coverage ----

    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Debug, PartialEq)]
    enum Observed {
        Close,
        Error(String),
        Panic,
    }

    fn record(seen: Arc<Mutex<Vec<Observed>>>) -> impl FnOnce(&Done) + Send + 'static {
        move |done| {
            let obs = match done {
                Done::Close => Observed::Close,
                Done::Error(e) => Observed::Error(e.to_string()),
                Done::Panic => Observed::Panic,
            };
            seen.lock().unwrap().push(obs);
        }
    }

    #[test]
    fn on_done_fires_close_on_clean_exit() {
        let mut thread = Thread::<'_, IoThread>::new();
        let node = thread
            .line(|w| MockLine::new(w))
            .input::<poll::Sync>()
            .output::<poll::Sync>();

        let mut input: InputHandle = Get::get(&node).unwrap();
        thread.add(node);

        let seen = Arc::new(Mutex::new(Vec::new()));
        thread.on_done(record(seen.clone()));

        std::thread::scope(|s| {
            let handle = thread.start(s);
            input.close().unwrap();
            let _ = handle.join();
        });

        assert_eq!(*seen.lock().unwrap(), vec![Observed::Close]);
    }

    #[test]
    fn on_done_multiple_callbacks_fire_in_registration_order() {
        let mut thread = Thread::<'_, IoThread>::new();
        let node = thread
            .line(|w| MockLine::new(w))
            .input::<poll::Sync>()
            .output::<poll::Sync>();

        let mut input: InputHandle = Get::get(&node).unwrap();
        thread.add(node);

        let order = Arc::new(Mutex::new(Vec::<u32>::new()));
        let o1 = order.clone();
        let o2 = order.clone();
        let o3 = order.clone();
        thread.on_done(move |_| o1.lock().unwrap().push(1));
        thread.on_done(move |_| o2.lock().unwrap().push(2));
        thread.on_done(move |_| o3.lock().unwrap().push(3));

        std::thread::scope(|s| {
            let handle = thread.start(s);
            input.close().unwrap();
            let _ = handle.join();
        });

        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    /// Closing the source should propagate through the async chain
    /// and close the output edge.
    #[test]
    fn close_propagates_through_chain() {
        let mut thread = Thread::<'_, IoThread>::new();

        let parent = thread.line(|w| MockLine::new(w)).input::<poll::Sync>();

        let mut input: InputHandle = Get::get(&parent).unwrap();

        let mut child = thread
            .line(|w| MockLine::new(w))
            .parent(parent)
            .output::<poll::Sync>();

        let output = Receiver::new();
        make_push(&mut child, &output).unwrap();

        thread.add(child);
        std::thread::scope(|s| {
            let handle = thread.start(s);

            input.push(Message::Data(1)).unwrap();
            assert_eq!(output.read_front().unwrap(), Message::Data(4));

            input.close().unwrap();

            let result = output.read_front();
            assert!(
                result.is_err(),
                "Expected Closed error after close propagation, got {:?}",
                result
            );

            assert!(matches!(handle.join(), Join::Ok));
        });
    }
}
