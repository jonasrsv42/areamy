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
use crate::poll::context::mock::noop_context;
use crate::poll::edge;
use crate::poll::graph::{Graph, GraphBuilder, Reservations};
use crate::poll::thread::runtime::Runtime;
use crate::poll::traits::AsyncParent;
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

/// The runtime `node` builds into, from the nodes `reservations` reserved.
fn built(
    node: impl GraphBuilder<'static, TestThread>,
    reservations: Reservations<TestThread>,
) -> Runtime<'static, TestThread> {
    let mut graph = reservations.local();
    Box::new(node).build(&mut graph).unwrap();
    graph.finish().unwrap().0
}

/// The same for `node` built as a parent, feeding a fresh edge.
fn built_as_parent(
    node: impl AsyncParent<
        'static,
        OutType = usize,
        SignalType = Trackable<&'static str>,
        ThreadIdType = TestThread,
    >,
    reservations: Reservations<TestThread>,
) -> Runtime<'static, TestThread> {
    let mut graph = reservations.local();
    let (sender, _receiver) = edge::channel(noop_local_waker(), None);
    Box::new(node).build(sender, &mut graph).unwrap();
    graph.finish().unwrap().0
}

/// Polls the built node at `id` once.
fn poll(
    runtime: &mut Runtime<'static, TestThread>,
    id: usize,
) -> Result<core::task::Poll<()>, Error> {
    let node = runtime.nodes[id].as_mut().expect("a node at every id");
    node.pollable.poll(&mut noop_context())
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
        _graph: &mut Graph<'static, TestThread>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

/// Macro to create a deferred biunion node with correct type annotation.
macro_rules! deferred {
    ($reservations:expr) => {
        Node::<_, _, _, PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>>::deferred(
            mock_factory(),
            $reservations,
        )
    };
}

// ============================================================
// GraphBuilder — Sync inputs
// ============================================================

#[test]
fn sync_sync_sync_builds_four_nodes() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations)
        .input::<Left>()
        .input::<Right>()
        .output();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

#[test]
fn sync_sync_deferred_builds_four_nodes() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations)
        .input::<Left>()
        .input::<Right>();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Async left, Sync right
// ============================================================

#[test]
fn async_left_sync_right_sync_output() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Left>())
        .unwrap()
        .input::<Right>()
        .output();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Sync left, Async right
// ============================================================

#[test]
fn sync_left_async_right_sync_output() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).input::<Left>();
    let node = MockParent.via(node.at::<Right>()).unwrap().output();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Both Async inputs
// ============================================================

#[test]
fn async_async_sync_output() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Left>())
        .unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap().output();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

// ============================================================
// AsyncParent — biunion consumed as parent
// ============================================================

#[test]
fn sync_sync_deferred_as_parent() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations)
        .input::<Left>()
        .input::<Right>();
    assert_eq!(built_as_parent(node, reservations).nodes.len(), 4);
}

#[test]
fn async_async_deferred_as_parent() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Left>())
        .unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    assert_eq!(built_as_parent(node, reservations).nodes.len(), 4);
}

// ============================================================
// Builder order — right resolved before left
// ============================================================

#[test]
fn right_async_then_left_sync() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Right>())
        .unwrap()
        .input::<Left>()
        .output();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

#[test]
fn right_async_then_left_async() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Right>())
        .unwrap();
    let node = MockParent.via(node.at::<Left>()).unwrap().output();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

// ============================================================
// GraphBuilder — Deferred sinks with async inputs
// ============================================================

#[test]
fn async_left_sync_right_deferred_sink() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Left>())
        .unwrap()
        .input::<Right>();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

#[test]
fn sync_left_async_right_deferred_sink() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).input::<Left>();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

#[test]
fn async_async_deferred_sink() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Left>())
        .unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

// ============================================================
// AsyncParent — mixed async inputs consumed as parent
// ============================================================

#[test]
fn async_left_sync_right_as_parent() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).at::<Left>())
        .unwrap()
        .input::<Right>();
    assert_eq!(built_as_parent(node, reservations).nodes.len(), 4);
}

#[test]
fn sync_left_async_right_as_parent() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).input::<Left>();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    assert_eq!(built_as_parent(node, reservations).nodes.len(), 4);
}

// ============================================================
// Builder order — output before inputs
// ============================================================

#[test]
fn output_then_sync_inputs() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations)
        .output()
        .input::<Right>()
        .input::<Left>();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

#[test]
fn output_then_async_inputs() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).output();
    let node = MockParent.via(node.at::<Left>()).unwrap();
    let node = MockParent.via(node.at::<Right>()).unwrap();
    assert_eq!(built(node, reservations).nodes.len(), 4);
}

// ============================================================
// Input slots are reserved at mint
// ============================================================

#[test]
fn a_minted_biunion_never_built_leaves_its_slots_empty() {
    let mut reservations = Reservations::new();
    let _orphan = deferred!(&mut reservations);
    match reservations.local::<'static>().finish() {
        Err(e) => assert!(e.to_string().contains("slot 0 is empty"), "{e}"),
        Ok(_) => panic!("an unbuilt biunion's reserved slots must fail the build"),
    }
}

#[test]
fn inputs_build_into_the_slots_reserved_at_mint() {
    let mut reservations = Reservations::new();
    let first = deferred!(&mut reservations);
    let second = deferred!(&mut reservations)
        .input::<Left>()
        .input::<Right>();
    // `first`'s sides resolve after `second` exists: one Async, one Sync.
    let first = MockParent.via(first.at::<Right>()).unwrap().input::<Left>();
    let mut graph = reservations.local();
    GraphBuilder::build(Box::new(second), &mut graph).unwrap();
    // Left and right reserved first (0, 1); work and output made at build.
    GraphBuilder::build(Box::new(first), &mut graph).unwrap();
    let (runtime, _) = graph.finish().unwrap();
    assert_eq!(runtime.nodes.len(), 8);
}

/// After 3 pushes into Left, one Left poll and one Output poll: what reached downstream.
fn delivered_in_one_round(budget: Budget) -> usize {
    let mut reservations = Reservations::new();
    let options = Options {
        budget,
        ..Options::default()
    };
    let mut node = Node::<
        _,
        _,
        _,
        PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>,
    >::with_options(mock_factory(), &mut reservations, options)
    .input::<Left>()
    .input::<Right>()
    .output();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
    let mut left: Box<WorkSink> = Get::<WorkSink, Left>::get(&node).unwrap();
    for value in 0..3 {
        left.push(Message::Data(value)).unwrap();
    }
    let mut runtime = built(node, reservations);
    // Built Left, Right, Work, Output.
    let _ = poll(&mut runtime, 0).unwrap();
    let _ = poll(&mut runtime, 3).unwrap();
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
        _graph: &mut Graph<'static, TestThread>,
    ) -> Result<(), Error> {
        for value in 0..3 {
            let _ = edge.try_push(Message::Data(value))?;
        }
        Ok(())
    }
}

/// Same, with Left's 3 messages from a parent: the parented build path.
fn delivered_from_a_parent_in_one_round(budget: Budget) -> usize {
    let mut reservations = Reservations::new();
    let options = Options {
        budget,
        ..Options::default()
    };
    let node = Node::<
        _,
        _,
        _,
        PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>,
    >::with_options(mock_factory(), &mut reservations, options);
    let node = PushingParent.via(node.at::<Left>()).unwrap();
    let mut node = MockParent.via(node.at::<Right>()).unwrap().output();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
    let mut runtime = built(node, reservations);
    // Built Left, Right, Work, Output. Left's edge closed as its parent was built.
    let _ = poll(&mut runtime, 0);
    let _ = poll(&mut runtime, 3);
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

/// A side that spends its input budget with messages left wakes its own slot for the rest,
/// never the other side's: the build hands each Input phase its own waker.
#[test]
fn a_side_out_of_budget_wakes_its_own_slot() {
    let mut reservations = Reservations::new();
    let options = Options {
        budget: Budget::default().input(NonZeroUsize::MIN),
        ..Options::default()
    };
    let mut node = Node::<
        _,
        _,
        _,
        PollBiunion<usize, usize, usize, Trackable<&'static str>, TestThread, _>,
    >::with_options(mock_factory(), &mut reservations, options)
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
    let mut graph = reservations.local();
    Box::new(node).build(&mut graph).unwrap();
    let (mut runtime, mut consumer) = graph.finish().unwrap();
    // Built Left, Right, Work, Output; past their first polls.
    let (left_id, right_id) = (0, 1);
    consumer.woken();

    let _ = poll(&mut runtime, left_id).unwrap();
    let after_left = consumer.woken();
    assert!(after_left.contains(&left_id), "{after_left:?}");
    assert!(!after_left.contains(&right_id), "{after_left:?}");

    let _ = poll(&mut runtime, right_id).unwrap();
    let after_right = consumer.woken();
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
