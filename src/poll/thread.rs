//! Async thread that runs [Pollable](crate::poll::Pollable) nodes driven by wakers.

pub(crate) mod runtime;
pub(crate) mod tls;

use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::poll::graph::{GraphBuilder, Reservations, Reserved};
use crate::poll::scheduler::Consumer;
use crate::thread::callback::{self, InitGuard, OnDone};
use crate::thread::done::Done;
use crate::thread::join::Spawned;
use crate::thread::{Join, Spawn, ThreadId};
use runtime::{Node, Runtime};
use std::thread::Scope;

/// An idle async thread. Add builders via [Thread::add], then
/// call [Thread::start] to spawn the OS thread.
///
/// One per [ThreadId] type: its reserved nodes and slots are typed by it alone.
pub struct Thread<'params, ThreadIdType: ThreadId> {
    builders: Vec<Box<dyn GraphBuilder<'params, ThreadIdType> + 'params>>,
    reservations: Reservations<ThreadIdType>,
    on_done: Vec<OnDone>,
}

/// Handle to a running async thread.
pub struct ThreadHandle<'threads> {
    spawned: Spawned<'threads>,
}

impl<'params, ThreadIdType: ThreadId> Default for Thread<'params, ThreadIdType> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'params, ThreadIdType: ThreadId> Thread<'params, ThreadIdType> {
    pub fn new() -> Self {
        Self {
            builders: Vec::new(),
            reservations: Reservations::new(),
            on_done: Vec::new(),
        }
    }

    /// Register a callback to fire when the thread exits. See
    /// [`ThreadStream::on_done`](crate::work::ThreadStream::on_done)
    /// for the full contract — same semantics here.
    pub fn on_done<F>(&mut self, callback: F) -> &mut Self
    where
        F: FnOnce(&Done) + Send + 'static,
    {
        self.on_done.push(Box::new(callback));
        self
    }

    /// Reserve a node before the thread starts: its waker goes to producers on other threads now.
    /// O(1): an `Arc` for its waker, and two `Arc` clones.
    ///
    /// A custom [`GraphBuilder`] claims each reserved node and adds a
    /// [`Pollable`](crate::poll::Pollable) to it; a node left out fails the build with "slot N is
    /// empty".
    pub fn reserve(&mut self) -> Reserved<ThreadIdType> {
        self.reservations.next()
    }

    /// For builders that reserve their nodes at mint.
    pub(crate) fn reservations(&mut self) -> &mut Reservations<ThreadIdType> {
        &mut self.reservations
    }

    /// Add a node to the thread. It will be built when the thread starts.
    pub fn add(&mut self, builder: impl GraphBuilder<'params, ThreadIdType> + 'params) {
        self.builders.push(Box::new(builder));
    }

    /// Run the poll loop synchronously on the current thread until every node closes, firing the
    /// `on_done` callbacks on exit. [`start`](Self::start) spawns it instead; a host that already
    /// owns a thread dedicated to this graph calls it directly.
    pub fn run(self) -> Result<(), Error> {
        self.into_task()()
    }

    /// The poll loop as a task for any thread. Its exit guard exists from here on, so `on_done`
    /// fires even if the task is dropped unrun because its spawn was refused.
    pub(crate) fn into_task(self) -> impl FnOnce() -> Result<(), Error> + Send + 'params {
        /// One capture, so drop order is the fields': a task dropped unrun drops its builders
        /// (they own the edges, closing them) before the guard fires `on_done`, as on every other
        /// exit.
        struct Unrun<'params, ThreadIdType: ThreadId> {
            builders: Vec<Box<dyn GraphBuilder<'params, ThreadIdType> + 'params>>,
            reservations: Reservations<ThreadIdType>,
            guard: InitGuard,
        }
        let Self {
            builders,
            reservations,
            on_done,
        } = self;
        let unrun = Unrun {
            builders,
            reservations,
            guard: InitGuard::new(ThreadIdType::name(), on_done),
        };
        move || {
            let Unrun {
                builders,
                reservations,
                guard,
            } = unrun;
            let guard = guard.start();
            let result = match prepare(builders, reservations) {
                Ok((mut runtime, mut consumer)) => poll_loop(&mut runtime, &mut consumer),
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
        self.with().start(scope)
    }

    /// Options for its own OS thread, such as [Spawn::stack_size]. The [Spawn] can only be started
    /// or added to a bundle, never run here, so the options always take effect.
    pub fn with(self) -> Spawn<Self> {
        Spawn::new(self)
    }
}

impl<'params, ThreadIdType: ThreadId> Spawn<Thread<'params, ThreadIdType>> {
    /// Start on its own OS thread, spawned into `scope` with these options.
    pub fn start<'threads>(
        self,
        scope: &'threads Scope<'threads, 'params>,
    ) -> ThreadHandle<'threads>
    where
        'params: 'threads,
    {
        let Spawn { thread, options } = self;
        ThreadHandle {
            spawned: options.spawn(scope, ThreadIdType::name(), thread.into_task()),
        }
    }
}

/// Build the graph into the runtime and the consumer for the poll loop.
fn prepare<'params, ThreadIdType: ThreadId>(
    builders: Vec<Box<dyn GraphBuilder<'params, ThreadIdType> + 'params>>,
    reservations: Reservations<ThreadIdType>,
) -> Result<(Runtime<'params, ThreadIdType>, Consumer), Error> {
    let mut graph = reservations.local();
    for builder in builders {
        builder.build(&mut graph)?;
    }
    graph.finish()
}

impl<'threads> ThreadHandle<'threads> {
    /// Join the async thread.
    pub fn join(self) -> Join {
        self.spawned.join()
    }
}

/// Waker-driven poll loop. Parks while no node is woken, polls only woken
/// nodes. A node returning `Ready` or `Closed` is taken out of its
/// slot here — its close-on-drop cascade fires immediately, and a
/// later wake for the same id is observable as `None`.
fn poll_loop<ThreadIdType: ThreadId>(
    runtime: &mut Runtime<ThreadIdType>,
    consumer: &mut Consumer,
) -> Result<(), Error> {
    let mut alive = runtime.nodes.len();

    while alive > 0 {
        let node_id = consumer.next();

        // A waker or timer made for an id outside this graph.
        let Some(slot) = runtime.nodes.get_mut(node_id) else {
            return Err(fatal!("woken for node {node_id}, not in this graph"));
        };
        let Some(Node { pollable, cx }) = slot.as_mut() else {
            // Benign: a cross-thread producer or sibling node can fire
            // this slot's waker after we eager-dropped the node on a
            // prior Ready/Closed. Silently skip.
            continue;
        };

        // Ambient timer for handle-free leaf futures (poll::sleep).
        let _current = tls::ThreadLocalGuard::set(cx.timer.clone());
        match pollable.poll(cx) {
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
    use crate::Trackable;
    use crate::edge::sync::Receiver;
    use crate::graph::Get;
    use crate::node::line::poll::routine::tests::MockLine;
    use crate::tests::support::{DEADLINE, deadline};
    use crate::work::Sink;
    use crate::{Closeable, Message, Pushes, Via};

    crate::thread_id!(IoThread);

    type InputHandle =
        Box<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>;

    #[test]
    fn async_thread_starts_and_stops() {
        let _deadline = deadline(DEADLINE);
        let mut thread = Thread::<IoThread>::new();

        let node = thread.line(MockLine::new).input().output();

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
        let _deadline = deadline(DEADLINE);
        let mut thread = Thread::<IoThread>::new();

        let mut node = thread.line(MockLine::new).input().output();

        let mut input: InputHandle = Get::get(&node).unwrap();

        let output = Receiver::new();
        node.pushes(&output).unwrap();

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

    use alloc::sync::Arc;
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
        let _deadline = deadline(DEADLINE);
        let mut thread = Thread::<IoThread>::new();
        let node = thread.line(MockLine::new).input().output();

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
        let _deadline = deadline(DEADLINE);
        let mut thread = Thread::<IoThread>::new();
        let node = thread.line(MockLine::new).input().output();

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

    /// Closing the writer should propagate through the async chain
    /// and close the output edge.
    #[test]
    fn close_propagates_through_chain() {
        let _deadline = deadline(DEADLINE);
        let mut thread = Thread::<IoThread>::new();

        let parent = thread.line(MockLine::new).input();

        let mut input: InputHandle = Get::get(&parent).unwrap();

        let mut child = parent.via(thread.line(MockLine::new)).unwrap().output();

        let output = Receiver::new();
        child.pushes(&output).unwrap();

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
