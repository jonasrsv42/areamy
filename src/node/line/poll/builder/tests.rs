//! Builder tests: GraphBuilder::build, AsyncParent::build, and builder ordering.

use super::node::Node;
use super::options::Options;
use super::types::PollLine;
use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::{Get, TryPushable};
use crate::message::Message;
use crate::node::line::poll::routine::tests::MockLine;
use crate::poll::Budget;
use crate::poll::context::mock::noop_context;
use crate::poll::edge::{self, Null};
use crate::poll::graph::Runtime;
use crate::poll::graph::{Graph, GraphBuilder, Reservations};
use crate::poll::traits::AsyncParent;
use crate::wake::mock::noop_local_waker;
use crate::{Pushable, Pushes, Trackable, Via, work};
use core::num::NonZeroUsize;

crate::thread_id!(TestThread);

fn mock_factory() -> impl FnOnce(crate::poll::LineWakers) -> MockLine + Send {
    |wakers| MockLine::new(wakers)
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

macro_rules! deferred {
    ($reservations:expr) => {
        Node::<_, _, PollLine<usize, usize, Trackable<&'static str>, TestThread, _>>::deferred(
            mock_factory(),
            $reservations,
        )
    };
}

// ============================================================
// GraphBuilder — input first, then output
// ============================================================

#[test]
fn input_sync_output_sync() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).input().output();
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

#[test]
fn input_sync_deferred_sink() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).input();
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

// ============================================================
// GraphBuilder — output first, then input
// ============================================================

#[test]
fn output_sync_input_sync() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).output().input();
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

#[test]
fn output_sync_then_parent() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations).output())
        .unwrap();
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

// ============================================================
// GraphBuilder — async input via parent
// ============================================================

#[test]
fn parent_output_sync() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations))
        .unwrap()
        .output();
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

#[test]
fn parent_deferred_sink() {
    let mut reservations = Reservations::new();
    let node = MockParent.via(deferred!(&mut reservations)).unwrap();
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

#[test]
fn multiple_parents_output_sync() {
    let mut reservations = Reservations::new();
    let node = MockParent.via(deferred!(&mut reservations)).unwrap();
    let node = MockParent.via(node).unwrap().output();
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

// ============================================================
// AsyncParent — consumed as parent
// ============================================================

#[test]
fn sync_deferred_as_parent() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).input();
    assert_eq!(built_as_parent(node, reservations).nodes.len(), 3);
}

#[test]
fn async_deferred_as_parent() {
    let mut reservations = Reservations::new();
    let node = MockParent.via(deferred!(&mut reservations)).unwrap();
    assert_eq!(built_as_parent(node, reservations).nodes.len(), 3);
}

// ============================================================
// GraphBuilder — direct sink output
// ============================================================

#[test]
fn input_sync_direct_sink() {
    let mut reservations = Reservations::new();
    let node = deferred!(&mut reservations).input().sink(Null::new());
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

#[test]
fn parent_direct_sink() {
    let mut reservations = Reservations::new();
    let node = MockParent
        .via(deferred!(&mut reservations))
        .unwrap()
        .sink(Null::new());
    assert_eq!(built(node, reservations).nodes.len(), 3);
}

// ============================================================
// Options reach the built phases
// ============================================================

type WorkSink = dyn work::Sink<DataType = usize, SignalType = Trackable<&'static str>>
    + Send
    + core::marker::Sync;

/// After 3 pushes, one Input poll and one Output poll: what reached downstream.
fn delivered_in_one_round(budget: Budget) -> usize {
    let mut reservations = Reservations::new();
    let options = Options {
        bound: None,
        budget,
    };
    let mut node =
        Node::<_, _, PollLine<usize, usize, Trackable<&'static str>, TestThread, _>>::with_options(
            mock_factory(),
            &mut reservations,
            options,
        )
        .input()
        .output();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
    let mut upstream: Box<WorkSink> = Get::get(&node).unwrap();
    for value in 0..3 {
        upstream.push(Message::Data(value)).unwrap();
    }
    let mut runtime = built(node, reservations);
    // Built Input, Work, Output.
    let _ = poll(&mut runtime, 0).unwrap();
    let _ = poll(&mut runtime, 2).unwrap();
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

/// Same, with the 3 messages from a parent's edge.
fn delivered_from_a_parent_in_one_round(budget: Budget) -> usize {
    let mut reservations = Reservations::new();
    let options = Options {
        bound: None,
        budget,
    };
    let mut node = PushingParent
        .via(Node::<
            _,
            _,
            PollLine<usize, usize, Trackable<&'static str>, TestThread, _>,
        >::with_options(
            mock_factory(), &mut reservations, options
        ))
        .unwrap()
        .output();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
    let mut runtime = built(node, reservations);
    // The parent's edge closed as it was built: draining it all ends in Closed.
    let _ = poll(&mut runtime, 0);
    let _ = poll(&mut runtime, 2);
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
    reservations: Reservations<TestThread>,
) {
    for value in 0..3 {
        upstream.push(Message::Data(value)).unwrap();
    }
    let mut runtime = built(node, reservations);
    let _ = poll(&mut runtime, 0).unwrap();
    let _ = poll(&mut runtime, 2).unwrap();
}

/// Options survive the other transition orders: output before input, and a direct sink.
#[test]
fn options_survive_output_first_and_a_sink() {
    let one = Options {
        bound: None,
        budget: Budget::default().output(NonZeroUsize::MIN),
    };

    let mut reservations = Reservations::new();
    let mut node =
        Node::<_, _, PollLine<usize, usize, Trackable<&'static str>, TestThread, _>>::with_options(
            mock_factory(),
            &mut reservations,
            one,
        )
        .output()
        .input();
    let downstream = Receiver::new();
    node.pushes(&downstream).unwrap();
    let upstream = Get::get(&node).unwrap();
    one_round(node, upstream, reservations);
    assert_eq!(downstream.len().unwrap(), 1);

    let mut reservations = Reservations::new();
    let downstream = Receiver::new();
    let node =
        Node::<_, _, PollLine<usize, usize, Trackable<&'static str>, TestThread, _>>::with_options(
            mock_factory(),
            &mut reservations,
            one,
        )
        .input()
        .sink(downstream.sender());
    let upstream = Get::get(&node).unwrap();
    one_round(node, upstream, reservations);
    assert_eq!(downstream.len().unwrap(), 1);
}

// ============================================================
// The input slot is reserved at mint
// ============================================================

#[test]
fn a_minted_node_never_built_leaves_its_slot_empty() {
    let mut reservations = Reservations::new();
    let _orphan = deferred!(&mut reservations);
    match reservations.local::<'static>().finish() {
        Err(e) => assert!(e.to_string().contains("slot 0 is empty"), "{e}"),
        Ok(_) => panic!("an unbuilt node's reserved slot must fail the build"),
    }
}

#[test]
fn nodes_wired_out_of_mint_order_fill_every_slot() {
    let mut reservations = Reservations::new();
    let first = deferred!(&mut reservations);
    let second = deferred!(&mut reservations).input();
    let first = MockParent.via(first).unwrap().output();
    let mut graph = reservations.local();
    GraphBuilder::build(Box::new(second), &mut graph).unwrap();
    // The parented input lands in the slot reserved at mint, before `second`'s.
    GraphBuilder::build(Box::new(first), &mut graph).unwrap();
    let (runtime, _) = graph.finish().unwrap();
    assert_eq!(runtime.nodes.len(), 6);
}
