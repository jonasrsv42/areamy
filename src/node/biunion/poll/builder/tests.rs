//! Builder tests: GraphBuilder::build, AsyncParent::build, and typestate transitions.

use super::node::Node;
use super::options::Options;
use super::types::PollBiunion;
use crate::biunion::{Left, Right};
use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::{Get, TryPushable};
use crate::message::Message;
use crate::node::biunion::poll::routine::tests::MockBiunion;
use crate::poll::Budget;
use crate::poll::NodeId;
use crate::poll::context::mock::noop_context;
use crate::poll::edge;
use crate::poll::graph::{Graph, GraphBuilder};
use crate::poll::scheduler::{Consumer, PollQueue, Producer};
use crate::poll::traits::AsyncParent;
use crate::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::wake::mock::noop_local_waker;
use crate::{At, Pushable, Pushes, Trackable, Via, work};
use core::num::NonZeroUsize;

crate::thread_id!(TestThread);

type WorkSink = dyn work::Sink<DataType = usize, SignalType = Trackable<&'static str>>
    + Send
    + core::marker::Sync;

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
        Node::<_, _, _, PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>>::deferred(
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
        .input::<Left>()
        .input::<Right>()
        .output();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_sync_deferred_builds_four_nodes() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Left>().input::<Right>();
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
    let node = MockParent
        .via(deferred!(&mut alloc).at::<Left>())
        .unwrap()
        .input::<Right>()
        .output();
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
    let node = deferred!(&mut alloc).input::<Left>();
    let node = MockParent.via(node.at::<Right>()).unwrap().output();
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
    let node = MockParent.via(deferred!(&mut alloc).at::<Left>()).unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap().output();
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
    let node = deferred!(&mut alloc).input::<Left>().input::<Right>();
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn async_async_deferred_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = MockParent.via(deferred!(&mut alloc).at::<Left>()).unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap();
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
    let node = MockParent
        .via(deferred!(&mut alloc).at::<Right>())
        .unwrap()
        .input::<Left>()
        .output();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn right_async_then_left_async() {
    let (mut alloc, to_local) = make_allocators();
    let node = MockParent.via(deferred!(&mut alloc).at::<Right>()).unwrap();
    let node = MockParent.via(node.at::<Left>()).unwrap().output();
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
    let node = MockParent
        .via(deferred!(&mut alloc).at::<Left>())
        .unwrap()
        .input::<Right>();
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_left_async_right_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Left>();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn async_async_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = MockParent.via(deferred!(&mut alloc).at::<Left>()).unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap();
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
    let node = MockParent
        .via(deferred!(&mut alloc).at::<Left>())
        .unwrap()
        .input::<Right>();
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_left_async_right_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).input::<Left>();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    let local = to_local(alloc);
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    let graph = AsyncParent::build(Box::new(node), sender, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// Builder order — output before inputs
// ============================================================

#[test]
fn output_then_sync_inputs() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .output()
        .input::<Right>()
        .input::<Left>();
    let graph = Box::new(node).build(to_local(alloc)).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn output_then_async_inputs() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).output();
    let node = MockParent.via(node.at::<Left>()).unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    let graph = Box::new(node).build(to_local(alloc)).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// Input slots are reserved at mint
// ============================================================

#[test]
fn a_minted_biunion_never_built_leaves_its_slots_empty() {
    let (mut alloc, to_local) = make_allocators();
    let _orphan = deferred!(&mut alloc);
    match to_local(alloc).build(Vec::new()) {
        Err(e) => assert!(e.to_string().contains("slot 0 is empty"), "{e}"),
        Ok(_) => panic!("an unbuilt biunion's reserved slots must fail the build"),
    }
}

#[test]
fn inputs_build_into_the_slots_reserved_at_mint() {
    let (mut alloc, to_local) = make_allocators();
    let first = deferred!(&mut alloc);
    let second = deferred!(&mut alloc).input::<Left>().input::<Right>();
    // `first`'s sides resolve after `second` exists: one Async, one Sync.
    let first = MockParent.via(first.at::<Right>()).unwrap().input::<Left>();
    let graph = GraphBuilder::build(Box::new(second), to_local(alloc)).unwrap();
    let mut nodes = graph.nodes;
    let graph = GraphBuilder::build(Box::new(first), graph.allocator).unwrap();
    let mut first_ids: Vec<_> = graph.nodes.iter().map(|n| n.id).collect();
    first_ids.sort();
    // Left and right reserved first (0, 1); work and output allocated at build.
    assert_eq!(first_ids[..2], [0, 1]);
    nodes.extend(graph.nodes);
    assert!(graph.allocator.build(nodes).is_ok());
}

// ============================================================
// Node IDs are unique
// ============================================================

#[test]
fn node_ids_are_unique() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<Left>()
        .input::<Right>()
        .output();
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
    let mut node = Node::<
        _,
        _,
        _,
        PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>,
    >::with_options(mock_factory(), &mut alloc, options)
    .input::<Left>()
    .input::<Right>()
    .output();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
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
    let node = Node::<
        _,
        _,
        _,
        PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>,
    >::with_options(mock_factory(), &mut alloc, options);
    let node = PushingParent.via(node.at::<Left>()).unwrap();
    let mut node = MockParent.via(node.at::<Right>()).unwrap().output();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
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

/// Ids woken since the last call: a sentinel pushed behind them marks the end, so this never
/// parks on an empty queue.
fn woken(consumer: &mut Consumer, remote: &Producer) -> Vec<NodeId> {
    const SENTINEL: NodeId = NodeId::MAX;
    remote.push(SENTINEL);
    core::iter::from_fn(|| Some(consumer.next()))
        .take_while(|&id| id != SENTINEL)
        .collect()
}

/// A side that spends its input budget with messages left wakes its own slot for the rest,
/// never the other side's: the build hands each Input phase its own waker.
#[test]
fn a_side_out_of_budget_wakes_its_own_slot() {
    let queue = PollQueue::new();
    let remote = queue.producer();
    let mut alloc = WakerAllocator::new(queue.producer());
    let (mut consumer, local) = queue.local();
    let options = Options {
        budget: Budget::default().input(NonZeroUsize::MIN),
        ..Options::default()
    };
    let mut node = Node::<
        _,
        _,
        _,
        PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>,
    >::with_options(mock_factory(), &mut alloc, options)
    .input::<Left>()
    .input::<Right>()
    .output();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
    let mut left: Box<WorkSink> = Get::<WorkSink, Left>::get(&node).unwrap();
    let mut right: Box<WorkSink> = Get::<WorkSink, Right>::get(&node).unwrap();
    for value in 0..3 {
        left.push(Message::Data(value)).unwrap();
        right.push(Message::Data(value)).unwrap();
    }
    let mut graph = Box::new(node).build(alloc.local(local)).unwrap();
    // Built Left, Right, Work, Output.
    let (left_id, right_id) = (graph.nodes[0].id, graph.nodes[1].id);
    woken(&mut consumer, &remote);
    let mut cx = noop_context();

    let _ = graph.nodes[0].pollable.poll(&mut cx).unwrap();
    let after_left = woken(&mut consumer, &remote);
    assert!(after_left.contains(&left_id), "{after_left:?}");
    assert!(!after_left.contains(&right_id), "{after_left:?}");

    let _ = graph.nodes[1].pollable.poll(&mut cx).unwrap();
    let after_right = woken(&mut consumer, &remote);
    assert!(after_right.contains(&right_id), "{after_right:?}");
    assert!(!after_right.contains(&left_id), "{after_right:?}");
}

#[test]
fn the_built_biunion_uses_its_budget() {
    assert_eq!(delivered_in_one_round(Budget::default()), 3);
    let input = Budget::default().input(NonZeroUsize::MIN);
    assert_eq!(delivered_in_one_round(input), 1);
    let output = Budget::default().output(NonZeroUsize::MIN);
    assert_eq!(delivered_in_one_round(output), 1);
}
