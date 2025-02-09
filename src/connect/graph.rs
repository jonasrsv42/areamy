use crate::error::Error;
use crate::marker::{Connection, Unary};
use crate::ThreadId;

/// -------------------------------------------------------------------------
/// Connect describes the traits we use chain computation and build our graph.
/// Aremy let's us build a graph of nodes that `ARE` **workable** and `HAVE`
/// **pushable** components, additionally we support single threaded **pullable** components.
/// It's important that nodes are not both **workable** and **pushable** as it would lead to
/// circular references and memory leaks.
///
///
/// Instead we enforce a structure where children always own
/// a reference to parents but parents never own references
/// to children, instead they only own a reference to
/// components of children that are `pushable`
///
/// Using `Pushable`, `Workable` and `Pullable` we can implement a hybrid
/// `Sync` and no-`Sync` graph. Where subgrahs without need for synchronization
/// can run efficiently without it and where synchronization is necessary it
/// can be used.
///
/// `Pullable` subgraphs are no-`Sync` while subgraphs relying on `Pushable` and `Workable`
/// are typically `Sync.`
/// `Sync` node variants. Can be used to connect `Workers` in a multi-threaded graph.
//
//          (output)
//         (thread 1)   (thread 2)
//     (push) ↑ ↓ (work) ↗ (work)
//            node node → →
//     (push) ↑ ↗ (work)  ↓
//            node        ↓
//     (push) ↑ ↓ (work)  ↓
//            node  ← ← ← ↓ (push)
//             ↑  (pull)
//            node
//             ↑ (pull)
//            node
//           (input)
///
///
/// Being `workable` implies that something can lend
/// its thread for computation. The contract
/// being that if you as a node are lended a thread
/// you should use that to try to produce output
/// pushed into the lenders `pushable`.
///
/// Our computation graph being a connection
/// of `Workable` implies our scheduling
/// will be a depth first search from
/// our output nodes towards data originating
/// from input nodes. The hope being that this
/// greedy scheduling will minimize latency
/// as we'll traverse up our tree as far as we
/// can everytime a node produces output.
///
/// A workable does not have to produce output on each work
/// but is encouraged to do so, since many basic nodes
/// will just work again if there's no output produced by parent.
///
/// We supply utility traits such as `Add`,
/// and `Get` to simplify convenience functions such as
/// `make_bidi`. A `bidi or bi-directional connection is a
/// connection between a parent and child where the child can
/// work the parent and parent can push into the child.
///
/// IMPORTANT is that a parent node itself should never
/// hold a reference to a child. Only the opposite is allowed.
/// This is easy to enforce by not implementing `Pushable` for
/// any node, but instead only for some container inside
/// nodes. All pre-implemented nodes follow this convention.
/// So as long as new nodes are not created that break this
/// convention there should be no problem.
///
/// All connections are not bidi, e.g. if nodes are owned
/// by different threads then the child may be `Pushable`
/// but the parent not `Workable` by the child, since the parent
/// would be worked by a different thread. Workable are templated
/// by thread-types as a mechanism to allow compile time
/// safety against work contention on nodes.
/// -------------------------------------------------------------------------

/// A `Workable` type can make ancenstors work, typically ancenstors are `Pushable` and
/// will push data forward when worked. This is how scheduling is done in the graph.
/// Threads are assigned to some leaf node and then children works the parents :).
///
/// Usually a work should yield data pushed into a nodes `Pushable`.
pub trait Workable: Send + Connection {
    // Thread associated with this `Workable`.
    type ThreadId: ThreadId;
    fn work(&mut self) -> Result<(), Error>;
}

/// A `Pushable` can have data pushed into it. Nodes
/// themselves should never be pushable. Instead nodes
/// hold a `SyncQueue` that is `Pushable` that can be
/// referenced by parents.
///
/// A typical behaviour is for a child to connect
/// to a parent by `make_bidi` which allows the
/// parent to borrow a reference to the childs `Pushable`
/// which tends to be a Arc<SyncQueue<...>> and for the
/// child to borrow a reference to the parent which is
/// `Workable`.
///
/// When the child then invokes the `Workable` the typical
/// expectation is for the parent to push data into the
/// childs `Pushable`.
///
/// NOTE! Nodes should never implement `Pushable` as it
/// easily leads to circualar references and memory leaks
/// see `line`, `biunion` and `bifurcation`. Instead
/// they hold a reference to something that
/// is `Pushable` such as a Arc<SyncQueue<...>>
/// Or Rc<RefCell<Vec<..>>>
pub trait Pushable: Sync + Send + Connection {
    type Message;

    fn push(&mut self, msg: Self::Message) -> Result<(), Error>;
}

/// trait `Add`(ing) an edge of a node type to a node.
/// All `Sync` node builders should implement this trait. E.g. look to `line`, `biunion` or `bifurcation`
/// as example.
///
/// As it allows nodes to recieve connections from other nodes.
/// E.g. for recieving outgoing or ingoing connections.
pub trait Add<ConnectionType: Connection + ?Sized, Multiplicity = Unary> {
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error>;
}

/// trait Get`(ting) an edge of a node type .
/// All `Sync` node builders should implement this trait. E.g. look to `line`, `biunion` or `bifurcation`
/// as example.
///
/// This allows to `Get` connections from a node and then `Add` it to another.
/// Such as getting one of its input queues and adding as output queue to a different node.
/// E.g. for recieving outgoing or ingoing connections.
pub trait Get<ConnectionType: Connection + ?Sized, Multiplicity = Unary> {
    fn get(&self) -> Result<Box<ConnectionType>, Error>;
}

// `Pullable` connections can be used in no-`Sync` segments of our
// graph to skip a few queues and mutexes.
//
// While the `Workable` and `Pushable` strategy is designed to be `Sync`.
// Commonly we have line segments of graphs that does not need
// to be `Sync` but only `Send`. The `Pullable` types lets us
// express the line-segments in subgraph without needed unnecessary
// syncronization. TL;DR it lets us skip a few mutexes and queues.
pub trait Pullable: Send + Connection {
    type ThreadId: ThreadId;
    type Message;

    fn pull(&mut self) -> Result<Self::Message, Error>;
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{fatal, make_bidi, make_push, DefaultThread, Message, NoPull, SyncQueue};
    use std::sync::{Arc, Mutex};

    // This is a `Stupid` coroutine. Coroutines are still experimental
    // at the time of writing 2024/12/11.
    pub struct Worker {
        state: usize,
    }

    impl Worker {
        fn work(&mut self, v: usize) -> usize {
            self.state = self.state + 1;
            return v * 2 + self.state;
        }
    }

    // `Sync` node variants. Can be used to connect `Workers` in a multi-threaded graph.
    //
    //   default thread
    //       ↑
    //      node  decoder thread
    //       ↑  ↗
    //      node  encoder thread
    //       ↑  ↗
    //      node (encoder)
    //       ↑
    //      node (audio input)
    //
    //  For example it allows us to implement a streaming speech graph
    //  where we spread audio encoding and state space search (decoding)
    //  into separate threads to improve latency.
    //
    pub struct SyncNode {
        pub input: Arc<SyncQueue<Message<usize, usize>>>,
        pub worker: Worker,
        pub workables: Vec<Box<dyn Workable<ThreadId = DefaultThread>>>,
        pub outputs: Vec<Box<dyn Pushable<Message = Message<usize, usize>>>>,
    }

    impl SyncNode {
        pub fn new() -> Self {
            SyncNode {
                input: Arc::new(SyncQueue::new()),
                worker: Worker { state: 0 },
                workables: Vec::new(),
                outputs: Vec::new(),
            }
        }
    }

    impl Workable for SyncNode {
        fn work(&mut self) -> Result<(), Error> {
            let is_empty = self.input.is_empty()?;
            if is_empty {
                for workable in self.workables.iter_mut() {
                    workable.work()?;
                }
            }

            let input_message = self.input.read_front()?;
            for output in self.outputs.iter_mut() {
                match input_message.clone() {
                    Message::Data(d) => output.push(Message::Data(self.worker.work(d)))?,
                    signal => output.push(signal)?,
                }
            }

            Ok(())
        }

        type ThreadId = DefaultThread;
    }

    impl Connection for SyncNode {}

    // Builder struct for constructing `Sync` graph(s).
    pub struct SyncBuilder(pub Arc<Mutex<SyncNode>>);

    impl Get<dyn Pushable<Message = Message<usize, usize>>> for SyncBuilder {
        fn get(&self) -> Result<Box<dyn Pushable<Message = Message<usize, usize>>>, Error> {
            let owned = self.0.lock().map_err(|e| fatal!(e))?;
            Ok(Box::new(owned.input.clone()))
        }
    }

    impl Get<dyn Workable<ThreadId = DefaultThread>> for SyncBuilder {
        fn get(&self) -> Result<Box<dyn Workable<ThreadId = DefaultThread>>, Error> {
            Ok(Box::new(self.0.clone()))
        }
    }

    impl Add<dyn Pushable<Message = Message<usize, usize>>> for SyncBuilder {
        fn add(
            &mut self,
            connection: Box<dyn Pushable<Message = Message<usize, usize>>>,
        ) -> Result<(), Error> {
            let mut owned = self.0.lock().map_err(|e| fatal!(e))?;
            Ok(owned.outputs.push(Box::new(connection)))
        }
    }
    impl Add<dyn Workable<ThreadId = DefaultThread>> for SyncBuilder {
        fn add(
            &mut self,
            connection: Box<dyn Workable<ThreadId = DefaultThread>>,
        ) -> Result<(), Error> {
            let mut owned = self.0.lock().map_err(|e| fatal!(e))?;
            Ok(owned.workables.push(connection))
        }
    }

    // NoSync variant of our node. Can be used to connect `Workers` without
    // synchronization such as condvars, arcs and mutexes
    //
    //   default thread
    //       ↑
    //      node
    //       ↑
    //      node
    //       ↑
    //      node
    pub struct NoSyncNode<PullableType>
    where
        PullableType: Pullable<ThreadId = DefaultThread, Message = Message<usize, usize>>,
    {
        pub input: Arc<SyncQueue<Message<usize, usize>>>,
        pub pullable: Option<PullableType>,
        pub worker: Worker,
    }

    impl<PullableType> NoSyncNode<PullableType>
    where
        PullableType: Pullable<ThreadId = DefaultThread, Message = Message<usize, usize>>,
    {
        pub fn new(pullable: PullableType) -> Self {
            Self {
                input: Arc::new(SyncQueue::new()),
                pullable: Some(pullable),
                worker: Worker { state: 0 },
            }
        }
    }

    impl NoSyncNode<NoPull<DefaultThread, Message<usize, usize>>> {
        pub fn root() -> Self {
            Self {
                input: Arc::new(SyncQueue::new()),
                pullable: None,
                worker: Worker { state: 0 },
            }
        }
    }

    impl<PullableType> Pullable for NoSyncNode<PullableType>
    where
        PullableType: Pullable<ThreadId = DefaultThread, Message = Message<usize, usize>>,
    {
        type ThreadId = DefaultThread;
        type Message = Message<usize, usize>;

        fn pull(&mut self) -> Result<Self::Message, Error> {
            let value = match &mut self.pullable {
                Some(pullable) => pullable.pull()?,
                None => self.input.read_front()?,
            };

            match value {
                Message::Data(d) => return Ok(Message::Data(self.worker.work(d))),
                signal => return Ok(signal),
            }
        }
    }

    impl<PullableType> Connection for NoSyncNode<PullableType> where
        PullableType: Pullable<ThreadId = DefaultThread, Message = Message<usize, usize>>
    {
    }

    // A chain that can be shared between threads.
    #[test]
    fn connect_sync_bidi_chain() {
        let mut node_1_builder = SyncBuilder(Arc::new(Mutex::new(SyncNode::new())));
        let mut node_2_builder = SyncBuilder(Arc::new(Mutex::new(SyncNode::new())));

        make_bidi(&mut node_1_builder, &mut node_2_builder).unwrap();

        let mut input =
            Get::<dyn Pushable<Message = Message<usize, usize>>>::get(&node_1_builder).unwrap();

        let sink = Arc::new(SyncQueue::new());

        make_push(&mut node_2_builder, &sink).unwrap();

        input.push(Message::Data(0)).unwrap();
        input.push(Message::Data(1)).unwrap();
        input.push(Message::Data(2)).unwrap();

        let mut node_2 = node_2_builder.0;
        node_2.work().unwrap();
        node_2.work().unwrap();
        node_2.work().unwrap();

        assert_eq!(
            sink.read_all().unwrap(),
            vec![Message::Data(3), Message::Data(10), Message::Data(17)]
        );
    }

    // A chain that is no-Sync, not using unnecessary mutexes, arcs and convars.
    #[test]
    fn connect_nosync_bidi_chain() {
        let node_1 = NoSyncNode::root();

        let mut input = node_1.input.clone();

        let mut node_2 = NoSyncNode::new(node_1);

        input.push(Message::Data(0)).unwrap();
        input.push(Message::Data(1)).unwrap();
        input.push(Message::Data(2)).unwrap();

        assert_eq!(node_2.pull().unwrap(), Message::Data(3));
        assert_eq!(node_2.pull().unwrap(), Message::Data(10));
        assert_eq!(node_2.pull().unwrap(), Message::Data(17));
    }
}
