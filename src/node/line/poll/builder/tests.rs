//! Builder tests: GraphBuilder::build, AsyncParent::build, and builder ordering.

use super::node::Node;
use super::options::Options;
use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::{Get, TryPushable};
use crate::message::Message;
use crate::node::line::poll::routine::tests::MockLine;
use crate::poll::Budget;
use crate::poll::context::mock::noop_context;
use crate::poll::edge::{self, Null, Sync};
use crate::poll::graph::{Graph, GraphBuilder};
use crate::poll::queue::PollQueue;
use crate::poll::traits::AsyncParent;
use crate::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::wake::mock::noop_local_waker;
use crate::{Push, Pushable, Trackable, work};
use std::num::NonZeroUsize;

crate::thread_id!(TestThread);

fn mock_factory() -> impl FnOnce(crate::poll::LineWakers) -> MockLine + Send {
    |wakers| MockLine::new(wakers)
}

fn make_allocators() -> (
    WakerAllocator,
    impl FnOnce(WakerAllocator) -> ThreadLocalWakerAllocator<TestThread>,
) {
    let queue = PollQueue::new();
    let producer = queue.producer();
    let (_, local_producer) = queue.local();
    let alloc = WakerAllocator::new(producer);
    (alloc, move |a: WakerAllocator| a.local(local_producer))
}

/// Minimal AsyncParent that produces zero nodes of its own.
struct MockParent;

impl AsyncParent<'static> for MockParent {
    type OutType = usize;
    type SignalType = Trackable<&'static str>;
    type ThreadIdType = TestThread;

    fn build(
        self: Box<Self>,
        _edge: edge::Sender<usize, Trackable<&'static str>>,
        allocator: ThreadLocalWakerAllocator<TestThread>,
    ) -> Result<Graph<'static, TestThread>, Error> {
        Ok(Graph {
            allocator,
            nodes: vec![],
        })
    }
}

macro_rules! deferred {
    ($alloc:expr) => {
        Node::<_, _, usize, usize, Trackable<&'static str>, TestThread, _>::deferred(
            mock_factory(),
            $alloc,
        )
    };
}

// ============================================================
// GraphBuilder — input first, then output
// ============================================================

#[test]
fn input_sync_output_sync() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Sync>().output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

#[test]
fn input_sync_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Sync>();
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

// ============================================================
// GraphBuilder — output first, then input
// ============================================================

#[test]
fn output_sync_input_sync() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).output::<Sync>().input::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

#[test]
fn output_sync_then_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).output::<Sync>().parent(MockParent);
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

// ============================================================
// GraphBuilder — async input via parent
// ============================================================

#[test]
fn parent_output_sync() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).parent(MockParent).output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

#[test]
fn parent_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).parent(MockParent);
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

#[test]
fn multiple_parents_output_sync() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent(MockParent)
        .parent(MockParent)
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

// ============================================================
// AsyncParent — consumed as parent
// ============================================================

#[test]
fn sync_deferred_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Sync>();
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

#[test]
fn async_deferred_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).parent(MockParent);
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

// ============================================================
// GraphBuilder — direct sink output
// ============================================================

#[test]
fn input_sync_direct_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Sync>().sink(Null::new());
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

#[test]
fn parent_direct_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).parent(MockParent).sink(Null::new());
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

// ============================================================
// Options reach the built phases
// ============================================================

type WorkSink = dyn work::Sink<DataType = usize, SignalType = Trackable<&'static str>>
    + Send
    + std::marker::Sync;

/// After 3 pushes, one Input poll and one Output poll: what reached downstream.
fn delivered_in_one_round(budget: Budget) -> usize {
    let (mut alloc, to_local) = make_allocators();
    let options = Options {
        bound: None,
        budget,
    };
    let mut node =
        Node::<_, _, usize, usize, Trackable<&'static str>, TestThread, _>::with_options(
            mock_factory(),
            &mut alloc,
            options,
        )
        .input::<Sync>()
        .output::<Sync>();
    let downstream = Receiver::new();
    Push::connect(&mut node, &downstream).unwrap();
    let mut upstream: Box<WorkSink> = Get::get(&node).unwrap();
    for value in 0..3 {
        upstream.push(Message::Data(value)).unwrap();
    }
    let mut graph = Box::new(node).build(to_local(alloc)).unwrap();
    let mut cx = noop_context();
    // Built Input, Work, Output.
    let _ = graph.nodes[0].pollable.poll(&mut cx).unwrap();
    let _ = graph.nodes[2].pollable.poll(&mut cx).unwrap();
    downstream.len().unwrap()
}

/// A parent that pushes 3 messages into its edge as it is built.
struct PushingParent;

impl AsyncParent<'static> for PushingParent {
    type OutType = usize;
    type SignalType = Trackable<&'static str>;
    type ThreadIdType = TestThread;

    fn build(
        self: Box<Self>,
        mut edge: edge::Sender<usize, Trackable<&'static str>>,
        allocator: ThreadLocalWakerAllocator<TestThread>,
    ) -> Result<Graph<'static, TestThread>, Error> {
        for value in 0..3 {
            let _ = edge.try_push(Message::Data(value))?;
        }
        Ok(Graph {
            allocator,
            nodes: vec![],
        })
    }
}

/// Same, with the 3 messages from a parent's edge.
fn delivered_from_a_parent_in_one_round(budget: Budget) -> usize {
    let (mut alloc, to_local) = make_allocators();
    let options = Options {
        bound: None,
        budget,
    };
    let mut node =
        Node::<_, _, usize, usize, Trackable<&'static str>, TestThread, _>::with_options(
            mock_factory(),
            &mut alloc,
            options,
        )
        .parent(PushingParent)
        .output::<Sync>();
    let downstream = Receiver::new();
    Push::connect(&mut node, &downstream).unwrap();
    let mut graph = Box::new(node).build(to_local(alloc)).unwrap();
    let mut cx = noop_context();
    // The parent's edge closed as it was built: draining it all ends in Closed.
    let _ = graph.nodes[0].pollable.poll(&mut cx);
    let _ = graph.nodes[2].pollable.poll(&mut cx);
    downstream.len().unwrap()
}

#[test]
fn a_built_parented_node_uses_its_budget() {
    assert_eq!(delivered_from_a_parent_in_one_round(Budget::default()), 3);
    let input = Budget::default().input(NonZeroUsize::MIN);
    assert_eq!(delivered_from_a_parent_in_one_round(input), 1);
    let output = Budget::default().output(NonZeroUsize::MIN);
    assert_eq!(delivered_from_a_parent_in_one_round(output), 1);
}

#[test]
fn the_built_node_uses_its_input_budget() {
    let one = Budget::default().input(NonZeroUsize::MIN);
    assert_eq!(delivered_in_one_round(Budget::default()), 3);
    assert_eq!(delivered_in_one_round(one), 1);
}

#[test]
fn the_built_node_uses_its_output_budget() {
    let one = Budget::default().output(NonZeroUsize::MIN);
    assert_eq!(delivered_in_one_round(one), 1);
}

/// Pushes 3 through `upstream`, builds `node`, polls Input then Output once.
fn one_round(
    node: impl GraphBuilder<'static, TestThread>,
    mut upstream: Box<WorkSink>,
    alloc: ThreadLocalWakerAllocator<TestThread>,
) {
    for value in 0..3 {
        upstream.push(Message::Data(value)).unwrap();
    }
    let mut graph = Box::new(node).build(alloc).unwrap();
    let mut cx = noop_context();
    let _ = graph.nodes[0].pollable.poll(&mut cx).unwrap();
    let _ = graph.nodes[2].pollable.poll(&mut cx).unwrap();
}

/// Options survive the other transition orders: output before input, and a direct sink.
#[test]
fn options_survive_output_first_and_a_sink() {
    let one = Options {
        bound: None,
        budget: Budget::default().output(NonZeroUsize::MIN),
    };

    let (mut alloc, to_local) = make_allocators();
    let mut node =
        Node::<_, _, usize, usize, Trackable<&'static str>, TestThread, _>::with_options(
            mock_factory(),
            &mut alloc,
            one,
        )
        .output::<Sync>()
        .input::<Sync>();
    let downstream = Receiver::new();
    Push::connect(&mut node, &downstream).unwrap();
    let upstream = Get::get(&node).unwrap();
    one_round(node, upstream, to_local(alloc));
    assert_eq!(downstream.len().unwrap(), 1);

    let (mut alloc, to_local) = make_allocators();
    let downstream = Receiver::new();
    let node = Node::<_, _, usize, usize, Trackable<&'static str>, TestThread, _>::with_options(
        mock_factory(),
        &mut alloc,
        one,
    )
    .input::<Sync>()
    .sink(downstream.sender());
    let upstream = Get::get(&node).unwrap();
    one_round(node, upstream, to_local(alloc));
    assert_eq!(downstream.len().unwrap(), 1);
}

// ============================================================
// Node IDs are unique
// ============================================================

#[test]
fn node_ids_are_unique() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Sync>().output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    let mut ids: Vec<_> = graph.nodes.iter().map(|n| n.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3);
}
