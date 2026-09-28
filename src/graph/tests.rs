//! Example nodes wiring the connection traits together.

use crate::Push;
use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Connection;
use crate::graph::{Add, Closeable, Get, Pushable, Sink, TryPush};
use crate::message::Message;
use crate::poll::Pollable;
use crate::poll::waker::Waker;
use crate::pull::Pullable;
use crate::signal::Trackable;
use crate::thread::DefaultThread;
use crate::work::{self, Workable};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Payload that counts its clones in a shared counter.
#[derive(Debug)]
pub struct Counted(pub Arc<AtomicUsize>);

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.0.fetch_add(1, Ordering::Relaxed);
        Counted(self.0.clone())
    }
}

/// A sink that accepts everything but fails to close, with its name as the error.
pub struct FailingClose(pub &'static str);

impl Connection for FailingClose {}

impl Pushable for FailingClose {
    type DataType = usize;
    type SignalType = Trackable<&'static str>;

    fn push(&mut self, _msg: Message<usize, Trackable<&'static str>>) -> Result<(), Error> {
        Ok(())
    }

    fn try_push(
        &mut self,
        _msg: Message<usize, Trackable<&'static str>>,
    ) -> Result<TryPush<Message<usize, Trackable<&'static str>>>, Error> {
        Ok(TryPush::Pushed)
    }
}

impl Closeable for FailingClose {
    fn close(&mut self) -> Result<(), Error> {
        Err(fatal!(self.0))
    }
}

/// A sink that holds at most `bound` messages: `try_push` hands the rest back, `push` always
/// takes them. Stands in for a bounded edge.
#[derive(Debug, Default)]
pub struct Bounded {
    pub bound: usize,
    pub items: Vec<Message<usize, Trackable<&'static str>>>,
}

impl Bounded {
    pub fn new(bound: usize) -> Self {
        Self {
            bound,
            items: Vec::new(),
        }
    }
}

impl Connection for Bounded {}

impl Pushable for Bounded {
    type DataType = usize;
    type SignalType = Trackable<&'static str>;

    fn push(&mut self, msg: Message<usize, Trackable<&'static str>>) -> Result<(), Error> {
        self.items.push(msg);
        Ok(())
    }

    fn try_push(
        &mut self,
        msg: Message<usize, Trackable<&'static str>>,
    ) -> Result<TryPush<Message<usize, Trackable<&'static str>>>, Error> {
        if self.items.len() >= self.bound {
            return Ok(TryPush::Full(msg));
        }
        self.items.push(msg);
        Ok(TryPush::Pushed)
    }
}

impl Closeable for Bounded {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

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
        self.state += 1;
        v * 2 + self.state
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
    pub input: Receiver<usize, Trackable<&'static str>>,
    /// The underyling routine of the node.
    pub routine: Routine,

    /// Incoming scheduling connection(s) `Workable` that we can
    /// invoke for data.
    pub workers: Vec<Box<dyn Workable<ThreadId = DefaultThread>>>,

    /// Incoming combo `Pullable` that we can invoke for data and scheduling.
    pub pullable: Option<
        Box<
            dyn Pullable<
                    ThreadId = DefaultThread,
                    DataType = usize,
                    SignalType = Trackable<&'static str>,
                >,
        >,
    >,

    /// Outgoing data connections. Lets us shovel data into our child nodes.
    pub outputs:
        Vec<Box<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>>,
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

impl Node {
    pub fn new() -> Self {
        Node {
            input: Receiver::new(),
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

// To enable graph building we must implement factory methods for it
//
// 1. For `get`ing its input to give to something else.
// 2. For `add`ing something elses input to its output.
// 3. For `add`ing something elses `Workable` for scheduling.

/// Method for fetching input. We put it in a `Box` for dynamic dispatch.
impl Get<dyn Pushable<DataType = usize, SignalType = Trackable<&'static str>>> for Node {
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = usize, SignalType = Trackable<&'static str>>>, Error>
    {
        Ok(Box::new(self.input.sender()))
    }
}

/// Get Closeable for input edge.
impl Get<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync> for Node {
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync>,
        Error,
    > {
        Ok(Box::new(self.input.sender()))
    }
}

/// Method adding something to output.
impl Add<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync> for Node {
    fn add(
        &mut self,
        connection: Box<
            dyn Sink<DataType = usize, SignalType = Trackable<&'static str>> + Send + Sync,
        >,
    ) -> Result<(), Error> {
        self.outputs.push(connection);
        Ok(())
    }
}
/// Method for adding a schedulable node to be worked on.
impl Add<dyn Workable<ThreadId = DefaultThread>> for Node {
    fn add(
        &mut self,
        connection: Box<dyn Workable<ThreadId = DefaultThread>>,
    ) -> Result<(), Error> {
        self.workers.push(connection);
        Ok(())
    }
}

/// A `work` and `push` chain example. Since message passing uses
/// [crate::sync::Sender] / [crate::sync::Receiver] pairs, the same chain
/// works across threads — see the multi-threaded graph example above.
#[test]
fn connect_push_work_bidi_chain() {
    let node_1 = Node::new();
    let mut node_2 = Node::new();
    let mut node_3 = Node::new();

    let mut input =
        Get::<dyn Pushable<DataType = usize, SignalType = Trackable<&'static str>>>::get(&node_1)
            .unwrap();

    work::Bidi::connect(node_1, &mut node_2).unwrap();
    work::Bidi::connect(node_2, &mut node_3).unwrap();

    let sink = Receiver::new();

    Push::connect(&mut node_3, &sink).unwrap();

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

// Now we can make out Node usable in a `Pull` graph with a few additional methods.

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
    type SignalType = Trackable<&'static str>;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        let value = match &mut self.pullable {
            Some(pullable) => pullable.pull()?,
            None => self.input.read_front()?,
        };

        match value {
            Message::Data(d) => Ok(Message::Data(self.routine.process(d))),
            Message::Flush(signal) => Ok(Message::Flush(signal)),
            Message::Marker(signal) => Ok(Message::Marker(signal)),
        }
    }
}

/// Graph building for `Pullable`
/// Method adding something to output.
impl
    Add<
        dyn Pullable<
                ThreadId = DefaultThread,
                DataType = usize,
                SignalType = Trackable<&'static str>,
            >,
    > for Node
{
    fn add(
        &mut self,
        connection: Box<
            dyn Pullable<
                    ThreadId = DefaultThread,
                    DataType = usize,
                    SignalType = Trackable<&'static str>,
                >,
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

    let mut input = node_1.input.sender();

    Add::<
        dyn Pullable<
                ThreadId = DefaultThread,
                DataType = usize,
                SignalType = Trackable<&'static str>,
            >,
    >::add(node_2.as_mut(), node_1)
    .unwrap();
    Add::<
        dyn Pullable<
                ThreadId = DefaultThread,
                DataType = usize,
                SignalType = Trackable<&'static str>,
            >,
    >::add(node_3.as_mut(), node_2)
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
    input: Receiver<usize, Trackable<&'static str>>,
    routine: Routine,
    outputs: Vec<usize>,
}

impl AsyncNode {
    fn new() -> Self {
        AsyncNode {
            input: Receiver::new(),
            routine: Routine { state: 0 },
            outputs: Vec::new(),
        }
    }
}

impl Connection for AsyncNode {}

impl Pollable for AsyncNode {
    type ThreadId = DefaultThread;
    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
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
    use crate::poll::waker::{Waker, mock};

    let mut node = AsyncNode::new();
    let mut input = node.input.sender();

    input.push(Message::Data(0)).unwrap();
    input.push(Message::Data(1)).unwrap();

    let mut waker = Waker {
        sync: std::task::Waker::noop().clone(),
        local: mock::noop_local_waker(),
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
    input: Receiver<usize, Trackable<&'static str>>,
}

impl Connection for ClosingAsyncNode {}

impl Pollable for ClosingAsyncNode {
    type ThreadId = DefaultThread;
    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        match self.input.poll() {
            Ok(Some(_)) => Ok(core::task::Poll::Pending),
            Ok(None) => Ok(core::task::Poll::Pending),
            Err(_) => Ok(core::task::Poll::Ready(())),
        }
    }
}

#[test]
fn pollable_returns_ready_on_close() {
    use crate::poll::waker::{Waker, mock};

    let mut node = ClosingAsyncNode {
        input: Receiver::new(),
    };

    node.input.close().unwrap();

    let mut waker = Waker {
        sync: std::task::Waker::noop().clone(),
        local: mock::noop_local_waker(),
    };

    assert!(matches!(
        node.poll(&mut waker).unwrap(),
        core::task::Poll::Ready(())
    ));
}
