//! Builder tests: GraphBuilder::build, AsyncParent::build, and typestate transitions.

use super::node::Node;
use super::options::Options;
use crate::biunion::{Left, Right};
use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::{Get, TryPushable};
use crate::message::Message;
use crate::node::biunion::poll::routine::tests::MockBiunion;
use crate::poll::Budget;
use crate::poll::context::mock::noop_context;
use crate::poll::edge::{self, Sync};
use crate::poll::graph::{Graph, GraphBuilder};
use crate::poll::queue::PollQueue;
use crate::poll::traits::AsyncParent;
use crate::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::wake::mock::noop_local_waker;
use crate::{Push, Pushable, Trackable, work};
use std::num::NonZeroUsize;

crate::thread_id!(TestThread);

type WorkSink = dyn work::Sink<DataType = usize, SignalType = Trackable<&'static str>>
    + Send
    + std::marker::Sync;

fn mock_factory() -> impl FnOnce(crate::poll::BiunionWakers) -> MockBiunion + Send {
    |wakers| MockBiunion::new(wakers)
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

/// Macro to create a deferred biunion node with correct type annotation.
macro_rules! deferred {
    ($alloc:expr) => {
        Node::<_, _, _, _, usize, usize, usize, Trackable<&'static str>, TestThread, _>::deferred(
            mock_factory(),
            $alloc,
        )
    };
}

// ============================================================
// GraphBuilder — Sync inputs
// ============================================================

#[test]
fn sync_sync_sync_builds_four_nodes() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left, Sync>()
        .input::<Right, Sync>()
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_sync_deferred_builds_four_nodes() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left, Sync>()
        .input::<Right, Sync>();
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Async left, Sync right
// ============================================================

#[test]
fn async_left_sync_right_sync_output() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Left>(MockParent)
        .input::<Right, Sync>()
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Sync left, Async right
// ============================================================

#[test]
fn sync_left_async_right_sync_output() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left, Sync>()
        .parent::<Right>(MockParent)
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Both Async inputs
// ============================================================

#[test]
fn async_async_sync_output() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Left>(MockParent)
        .parent::<Right>(MockParent)
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// AsyncParent — biunion consumed as parent
// ============================================================

#[test]
fn sync_sync_deferred_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left, Sync>()
        .input::<Right, Sync>();
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn async_async_deferred_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Left>(MockParent)
        .parent::<Right>(MockParent);
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// Builder order — right resolved before left
// ============================================================

#[test]
fn right_async_then_left_sync() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Right>(MockParent)
        .input::<Left, Sync>()
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn right_async_then_left_async() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Right>(MockParent)
        .parent::<Left>(MockParent)
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Deferred sinks with async inputs
// ============================================================

#[test]
fn async_left_sync_right_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Left>(MockParent)
        .input::<Right, Sync>();
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_left_async_right_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left, Sync>()
        .parent::<Right>(MockParent);
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn async_async_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Left>(MockParent)
        .parent::<Right>(MockParent);
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// AsyncParent — mixed async inputs consumed as parent
// ============================================================

#[test]
fn async_left_sync_right_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<Left>(MockParent)
        .input::<Right, Sync>();
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_left_async_right_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left, Sync>()
        .parent::<Right>(MockParent);
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// Node IDs are unique
// ============================================================

#[test]
fn node_ids_are_unique() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left, Sync>()
        .input::<Right, Sync>()
        .output::<Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    let mut ids: Vec<_> = graph.nodes.iter().map(|n| n.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4);
}

/// After 3 pushes into Left, one Left poll and one Output poll: what reached downstream.
fn delivered_in_one_round(budget: Budget) -> usize {
    let (mut alloc, to_local) = make_allocators();
    let options = Options {
        budget,
        ..Options::default()
    };
    let mut node = Node::<_, _, _, _, usize, usize, usize, Trackable<&'static str>, TestThread, _>::with_options(
        mock_factory(),
        &mut alloc,
        options,
    )
    .input::<Left, Sync>()
    .input::<Right, Sync>()
    .output::<Sync>();
    let downstream = Receiver::new();
    Push::connect(&mut node, &downstream).unwrap();
    let mut left: Box<WorkSink> = Get::<WorkSink, Left>::get(&node).unwrap();
    for value in 0..3 {
        left.push(Message::Data(value)).unwrap();
    }
    let mut graph = Box::new(node).build(to_local(alloc)).unwrap();
    let mut cx = noop_context();
    // Built Left, Right, Work, Output.
    let _ = graph.nodes[0].pollable.poll(&mut cx).unwrap();
    let _ = graph.nodes[3].pollable.poll(&mut cx).unwrap();
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

/// Same, with Left's 3 messages from a parent: the parented build path.
fn delivered_from_a_parent_in_one_round(budget: Budget) -> usize {
    let (mut alloc, to_local) = make_allocators();
    let options = Options {
        budget,
        ..Options::default()
    };
    let mut node = Node::<_, _, _, _, usize, usize, usize, Trackable<&'static str>, TestThread, _>::with_options(
        mock_factory(),
        &mut alloc,
        options,
    )
    .parent::<Left>(PushingParent)
    .parent::<Right>(MockParent)
    .output::<Sync>();
    let downstream = Receiver::new();
    Push::connect(&mut node, &downstream).unwrap();
    let mut graph = Box::new(node).build(to_local(alloc)).unwrap();
    let mut cx = noop_context();
    // Built Left, Right, Work, Output. Left's edge closed as its parent was built.
    let _ = graph.nodes[0].pollable.poll(&mut cx);
    let _ = graph.nodes[3].pollable.poll(&mut cx);
    downstream.len().unwrap()
}

#[test]
fn the_built_parented_biunion_uses_its_budget() {
    assert_eq!(delivered_from_a_parent_in_one_round(Budget::default()), 3);
    let input = Budget::default().input(NonZeroUsize::MIN);
    assert_eq!(delivered_from_a_parent_in_one_round(input), 1);
    let output = Budget::default().output(NonZeroUsize::MIN);
    assert_eq!(delivered_from_a_parent_in_one_round(output), 1);
}

#[test]
fn the_built_biunion_uses_its_budget() {
    assert_eq!(delivered_in_one_round(Budget::default()), 3);
    let input = Budget::default().input(NonZeroUsize::MIN);
    assert_eq!(delivered_in_one_round(input), 1);
    let output = Budget::default().output(NonZeroUsize::MIN);
    assert_eq!(delivered_in_one_round(output), 1);
}
