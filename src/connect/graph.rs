//! Graph connections and building blocks.
use crate::ThreadId;
use crate::connect::waker::Waker;
use crate::error::Error;
use crate::marker::{Connection, Multiplicity, Unary};
use crate::message::Message;
use crate::signal::Origin;

/// A [`Workable`] is a [Connection] in our graph that is used to for scheduling.
/// Child nodes will invoke the [Workable] connection to parents to make parent work.
///
/// The [Workable] has an associated [Workable::ThreadId] to indicate the unique thread that
/// is allowed to [Workable::work] on this node. By marking nodes we can ensure we don't have
/// threads contending on nodes.
pub trait Workable: Send + Connection {
    // Thread associated with this `Workable`.
    type ThreadId: ThreadId;
    fn work(&mut self) -> Result<(), Error>;
}

/// A [`Pushable`] is a data [Connection] in our graph, it is used for dataflow.
/// The associated [Pushable::DataType] and [Pushable::SignalType] types are used to specify
/// the types for the [Message<DataType, SignalType>] that can be [Pushable::push]ed through this [Connection].
///
/// Child nodes will typically hold [Pushable] referencs to queues owned by a parent
/// and [Pushable::push] [Message] into them when scheduled.
///
/// <div class="warning"> Nodes should never implement [Pushable] as it
/// easily leads to circualar references and memory leaks </div>
///
/// Instead nodes hold a reference to something that is `Pushable` such as a Arc<SyncEdge<...>> Or Rc<RefCell<Vec<..>>>
pub trait Pushable: Connection {
    /// The DataType used in [Message<DataType, SignalType>] for this [Pushable]
    type DataType;

    /// The SignalType used in [Message<DataType, SignalType>] for this [Pushable]
    type SignalType: Origin;

    /// Pushes a message to this [Pushable]
    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error>;
}

/// A [`Closeable`] is a [Pushable] that can be closed to signal no more data will be sent.
///
/// When a node receives an error from upstream (via pull/work connections), it should
/// close all its [Closeable] outputs to propagate the shutdown through push connections.
/// This enables clean shutdown cascade through the entire graph.
///
/// The exact semantics of close (e.g., first-close-wins vs refcounted) are up to the
/// implementation.
pub trait Closeable: Pushable {
    /// Close this connection, signaling no more data will be sent.
    fn close(&mut self) -> Result<(), Error>;
}

/// [`Pullable`] combines [Workable] and [Pushable] in being both a scheduling and dataflow
/// [Connection]. As with [Workable] the [Pullable] connection has a unique [Pullable::ThreadId] associated
/// with it as well as a [Message<DataType, SignalType>] that it will yield upon scheduling.
///
/// A [Pullable] is less flexible but is useful to declare line segments in the graph. It lets
/// us skip the syncronization parts of [Pushable] connections.
///
/// TL;DR it lets us skip a few mutexes, condvars and moves.
pub trait Pullable: Send + Connection {
    /// The thread that is allowed to schedule this node.
    type ThreadId: ThreadId;
    /// The DataType used in [Message<DataType, SignalType>] for this [Pullable]
    type DataType: Send + Sync;
    /// The SignalType used in [Message<DataType, SignalType>] for this [Pullable]
    type SignalType: Origin + Send + Sync;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error>;
}

/// [`Pollable`] is a [Connection] for event-driven nodes.
///
/// Unlike [Workable] which blocks until work is done, [Pollable::poll] is non-blocking
/// and receives a [Waker](crate::connect::waker::Waker) carrying both a sync waker
/// (for I/O registration / standard futures) and a thread-local waker (for cheap
/// same-thread wake).
///
/// Like [Workable], [Pollable] has an associated [Pollable::ThreadId] to ensure
/// nodes are only added to matching threads (compile-time safety).
pub trait Pollable: Connection {
    type ThreadId: ThreadId;
    fn poll(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error>;
}

/// [`Receivable`] is a non-blocking connection.
///  
/// This [`Connection`] checks if data is available, without blocking,
/// and return is there is or [`None`] if there's none
///
/// In [`Pollable`] graphs we do not couple edges with scheduling any
/// non-awaitable edge is therefore [`Receivable`]
///
pub trait Receivable: Connection {
    type DataType;
    type SignalType: Origin;

    /// Non-blocking receive.
    /// Returns `Ok(Some(msg))` if data is available.
    /// Returns `Ok(None)` if open but empty.
    /// Returns `Err(Closed)` if closed and empty.
    fn try_recv(&mut self) -> Result<Option<Message<Self::DataType, Self::SignalType>>, Error>;
}

/// [`Add`] trait is used to create [Connection]s between our nodes.
/// it also lets us declare the type of [Connection]s that nodes are
/// expected to have through generics with dynamic dispatch.
///
/// [Add] is generic over [Connection] to allow [Add::add] different types, for example
/// [Workable] or [Pushable]. It is also ?Sized because we make liberal use of
/// dynamic dispatch, hence a common pattern is to [Add::add] `dyn Trait`.
///
/// [Add] is generic over [Multiplicity] to allow [Add::add] multiple inbound and outbound
/// [Connection] for a single node. Per default all implementations are [Unary] unless
/// otherwise stated to avoid specifying [Multiplicity] where not necessary.
pub trait Add<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity = Unary> {
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error>;
}

/// [`Get`] trait is used for retrieving connection(s) from a node that can be added to others..
///
/// This allows to [Get] connections from a node and then [Add] it to another.
/// Such as getting one of its input queues and adding as output queue to a different node.
/// E.g. for recieving outgoing or ingoing connections.
///
/// [Get] is generic over [Connection] to allow [Get::get] different types, for example
/// [Pushable]. It is also ?Sized because we make liberal use of
/// dynamic dispatch, hence a common pattern is to [Get::get] a `dyn Trait`.
///
/// [Get] is generic over [Multiplicity] to allow [Get::get] multiple inbound
/// [Connection] for a single node. Per default all implementations are [Unary] unless
/// otherwise stated to avoid specifying [Multiplicity] where not necessary.
pub trait Get<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity = Unary> {
    fn get(&self) -> Result<Box<ConnectionType>, Error>;
}

#[cfg(any(test, doc))]
pub mod tests {
    use super::*;
    use crate::{DefaultThread, Message, SyncEdge, make_bidi, make_push};
    use std::sync::Arc;

    /// A `Simple` "coroutine". At time of writing, 2024/12/11, coroutines
    /// are still experimental in rust.
    pub struct Routine {
        state: usize,
    }

    /// Our routines processes digits but adding to its own state
    /// and then returning a digit (D * 2 + self.state). Showcasing
    /// it as a stateful function
    impl Routine {
        fn process(&mut self, v: usize) -> usize {
            self.state = self.state + 1;
            return v * 2 + self.state;
        }
    }

    /// `Node`  variants. Can be used to connect `Routine(s)` in a multi-threaded graph.
    ///
    /// ```bash
    ///   default thread
    ///       ↑
    ///      node  decoder thread
    ///       ↑  ↗
    ///      node  encoder thread
    ///       ↑  ↗
    ///      node (encoder)
    ///       ↑
    ///      node (audio input)
    /// ```
    ///  For example it allows us to implement a streaming graph
    ///  where we spread encoding and decoding
    ///  into separate threads to improve latency.
    ///
    pub struct Node {
        /// Incoming data connection(s), `Pushable`(s).
        pub input: Arc<SyncEdge<usize, usize>>,
        /// The underyling routine of the node.
        pub routine: Routine,

        /// Incoming scheduling connection(s) `Workable` that we can
        /// invoke for data.
        pub workers: Vec<Box<dyn Workable<ThreadId = DefaultThread>>>,

        /// Incoming combo `Pullable` that we can invoke for data and scheduling.
        pub pullable: Option<
            Box<dyn Pullable<ThreadId = DefaultThread, DataType = usize, SignalType = usize>>,
        >,

        /// Outgoing data connections. Lets us shovel data into our child nodes.
        pub outputs: Vec<Box<dyn Closeable<DataType = usize, SignalType = usize> + Send + Sync>>,
    }

    impl Node {
        pub fn new() -> Self {
            Node {
                input: Arc::new(SyncEdge::new()),
                routine: Routine { state: 0 },
                workers: Vec::new(),
                pullable: None,
                outputs: Vec::new(),
            }
        }
    }

    /// Mark that our node will act as a connection in a graph.
    impl Connection for Node {}

    /// Let's make our Node capable of being part of a `push`, `pull` graph by implementing
    /// `Workable` and `Pushable`
    impl Workable for Node {
        fn work(&mut self) -> Result<(), Error> {
            let is_empty = self.input.is_empty()?;
            if is_empty {
                for workable in self.workers.iter_mut() {
                    workable.work()?;
                }
            }

            let input_message = self.input.read_front()?;
            for output in self.outputs.iter_mut() {
                match input_message.clone() {
                    Message::Data(d) => output.push(Message::Data(self.routine.process(d)))?,
                    Message::Flush(signal) => output.push(Message::Flush(signal))?,
                    Message::Marker(signal) => output.push(Message::Marker(signal))?,
                }
            }

            Ok(())
        }

        type ThreadId = DefaultThread;
    }

    /// To enable graph building we must implement factory methods for it
    ///
    /// 1. For `get`ing its input to give to something else.
    /// 2. For `add`ing something elses input to its output.
    /// 3. For `add`ing something elses `Workable` for scheduling.

    /// Method for fetching input. We put it in a `Box` for dynamic dispatch.
    impl Get<dyn Pushable<DataType = usize, SignalType = usize>> for Node {
        fn get(&self) -> Result<Box<dyn Pushable<DataType = usize, SignalType = usize>>, Error> {
            Ok(Box::new(self.input.clone()))
        }
    }

    /// Get Closeable for input edge.
    impl Get<dyn Closeable<DataType = usize, SignalType = usize> + Send + Sync> for Node {
        fn get(
            &self,
        ) -> Result<Box<dyn Closeable<DataType = usize, SignalType = usize> + Send + Sync>, Error>
        {
            Ok(Box::new(self.input.clone()))
        }
    }

    /// Method adding something to output.
    impl Add<dyn Closeable<DataType = usize, SignalType = usize> + Send + Sync> for Node {
        fn add(
            &mut self,
            connection: Box<dyn Closeable<DataType = usize, SignalType = usize> + Send + Sync>,
        ) -> Result<(), Error> {
            Ok(self.outputs.push(connection))
        }
    }
    /// Method for adding a schedulable node to be worked on.
    impl Add<dyn Workable<ThreadId = DefaultThread>> for Node {
        fn add(
            &mut self,
            connection: Box<dyn Workable<ThreadId = DefaultThread>>,
        ) -> Result<(), Error> {
            Ok(self.workers.push(connection))
        }
    }

    /// A `work` and `push` chain example. Since we use `SyncEdge` for message passing
    /// it is excellent for sharing nodes across different threads. As in the mutli-threaded graph
    /// example above.
    #[test]
    fn connect_push_work_bidi_chain() {
        let node_1 = Box::new(Node::new());
        let mut node_2 = Box::new(Node::new());
        let mut node_3 = Box::new(Node::new());

        let mut input =
            Get::<dyn Pushable<DataType = usize, SignalType = usize>>::get(&node_1).unwrap();

        make_bidi(node_1, node_2.as_mut()).unwrap();
        make_bidi(node_2, node_3.as_mut()).unwrap();

        let sink = Arc::new(SyncEdge::new());

        make_push(&mut node_3, &sink).unwrap();

        input.push(Message::Data(0)).unwrap();
        input.push(Message::Data(1)).unwrap();
        input.push(Message::Data(2)).unwrap();

        node_3.work().unwrap();
        node_3.work().unwrap();
        node_3.work().unwrap();

        // 7 =
        //   Node 1 (0 * 2 + 1) = 1
        //   Node 2 (1 * 2 + 1) = 3
        //   Node 3 (3 * 2 + 1) = 7
        //
        // 22 =
        //  Node 1 (1 * 2 + 2) = 4
        //  Node 2 (4 * 2 + 2) = 10
        //  Node 3 (10 * 2 + 2) = 22
        //
        // 37 =
        //  Node 1 (2 * 2 + 3) = 7
        //  Node 2 (7 * 2 + 3) = 17
        //  Node 3 (17 * 2 + 3) = 37
        assert_eq!(
            sink.read_all().unwrap(),
            vec![Message::Data(7), Message::Data(22), Message::Data(37)]
        );
    }

    /// Now we can make out Node usable in a `Pull` graph with a few additional methods.

    /// Such a variant of a graph can be used to connect `Workers` without
    /// synchronization such as condvars, arcs and mutexes
    ///
    ///   default thread
    ///       ↑
    ///      node
    ///       ↑
    ///      node
    ///       ↑
    ///      node
    ///
    /// A pull graph can easily be used without dynamic dispatch if all connection(s) are unary.
    /// See the node -> line -> nosync -> node for an example. This can be useful for hotpath(s) of
    /// the graph if small messages are passed.
    impl Pullable for Node {
        type ThreadId = DefaultThread;
        type DataType = usize;
        type SignalType = usize;

        fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
            let value = match &mut self.pullable {
                Some(pullable) => pullable.pull()?,
                None => self.input.read_front()?,
            };

            match value {
                Message::Data(d) => return Ok(Message::Data(self.routine.process(d))),
                Message::Flush(signal) => return Ok(Message::Flush(signal)),
                Message::Marker(signal) => return Ok(Message::Marker(signal)),
            }
        }
    }

    /// Graph building for `Pullable`
    /// Method adding something to output.
    impl Add<dyn Pullable<ThreadId = DefaultThread, DataType = usize, SignalType = usize>> for Node {
        fn add(
            &mut self,
            connection: Box<
                dyn Pullable<ThreadId = DefaultThread, DataType = usize, SignalType = usize>,
            >,
        ) -> Result<(), Error> {
            self.pullable = Some(connection);

            Ok(())
        }
    }

    /// A chain that has `pull` connection, not using unnecessary mutexes, arcs and convars.
    #[test]
    fn connect_pull_bidi_chain() {
        let node_1 = Box::new(Node::new());
        let mut node_2 = Box::new(Node::new());
        let mut node_3 = Box::new(Node::new());

        let mut input = node_1.input.clone();

        Add::<dyn Pullable<ThreadId = DefaultThread, DataType = usize, SignalType = usize>>::add(
            node_2.as_mut(),
            node_1,
        )
        .unwrap();
        Add::<dyn Pullable<ThreadId = DefaultThread, DataType = usize, SignalType = usize>>::add(
            node_3.as_mut(),
            node_2,
        )
        .unwrap();

        input.push(Message::Data(0)).unwrap();
        input.push(Message::Data(1)).unwrap();
        input.push(Message::Data(2)).unwrap();

        assert_eq!(node_3.pull().unwrap(), Message::Data(7));
        assert_eq!(node_3.pull().unwrap(), Message::Data(22));
        assert_eq!(node_3.pull().unwrap(), Message::Data(37));
    }

    /// A simple async node that processes input via `Pollable`.
    /// It polls its input edge non-blockingly and processes data.
    struct AsyncNode {
        input: Arc<SyncEdge<usize, usize>>,
        routine: Routine,
        outputs: Vec<usize>,
    }

    impl AsyncNode {
        fn new() -> Self {
            AsyncNode {
                input: Arc::new(SyncEdge::new()),
                routine: Routine { state: 0 },
                outputs: Vec::new(),
            }
        }
    }

    impl Connection for AsyncNode {}

    impl Pollable for AsyncNode {
        type ThreadId = DefaultThread;
        fn poll(
            &mut self,
            _waker: &mut crate::connect::waker::Waker,
        ) -> Result<core::task::Poll<()>, Error> {
            match self.input.poll()? {
                Some(Message::Data(d)) => {
                    self.outputs.push(self.routine.process(d));
                    Ok(core::task::Poll::Pending)
                }
                Some(_) => Ok(core::task::Poll::Pending),
                None => Ok(core::task::Poll::Pending),
            }
        }
    }

    /// A `Pollable` node that processes input non-blockingly.
    #[test]
    fn pollable_processes_available_input() {
        use crate::connect::waker::{ThreadLocalWake, ThreadLocalWaker, Waker};

        struct NoopWake;
        impl ThreadLocalWake for NoopWake {
            fn wake(&self) {}
        }

        let mut node = AsyncNode::new();
        let mut input = node.input.clone();

        input.push(Message::Data(0)).unwrap();
        input.push(Message::Data(1)).unwrap();

        let mut waker = Waker {
            sync: std::task::Waker::noop().clone(),
            local: ThreadLocalWaker::new(NoopWake),
        };

        // First poll processes first message
        assert!(matches!(
            node.poll(&mut waker).unwrap(),
            core::task::Poll::Pending
        ));
        assert_eq!(node.outputs, vec![1]); // 0 * 2 + 1

        // Second poll processes second message
        assert!(matches!(
            node.poll(&mut waker).unwrap(),
            core::task::Poll::Pending
        ));
        assert_eq!(node.outputs, vec![1, 4]); // 1 * 2 + 2

        // Third poll finds no input — still pending
        assert!(matches!(
            node.poll(&mut waker).unwrap(),
            core::task::Poll::Pending
        ));
        assert_eq!(node.outputs, vec![1, 4]); // unchanged
    }

    /// A `Pollable` node that returns `Ready` when closed.
    struct ClosingAsyncNode {
        input: Arc<SyncEdge<usize, usize>>,
    }

    impl Connection for ClosingAsyncNode {}

    impl Pollable for ClosingAsyncNode {
        type ThreadId = DefaultThread;
        fn poll(
            &mut self,
            _waker: &mut crate::connect::waker::Waker,
        ) -> Result<core::task::Poll<()>, Error> {
            match self.input.poll() {
                Ok(Some(_)) => Ok(core::task::Poll::Pending),
                Ok(None) => Ok(core::task::Poll::Pending),
                Err(_) => Ok(core::task::Poll::Ready(())),
            }
        }
    }

    #[test]
    fn pollable_returns_ready_on_close() {
        use crate::connect::waker::{ThreadLocalWake, ThreadLocalWaker, Waker};

        struct NoopWake;
        impl ThreadLocalWake for NoopWake {
            fn wake(&self) {}
        }

        let mut node = ClosingAsyncNode {
            input: Arc::new(SyncEdge::new()),
        };

        node.input.close().unwrap();

        let mut waker = Waker {
            sync: std::task::Waker::noop().clone(),
            local: ThreadLocalWaker::new(NoopWake),
        };

        assert!(matches!(
            node.poll(&mut waker).unwrap(),
            core::task::Poll::Ready(())
        ));
    }
}
