//! Builder tests: GraphBuilder::build, AsyncParent::build, and builder ordering.

use super::node::Node;
use crate::ThreadId;
use crate::connect::poll::edge::{PollEdge, Sync};
use crate::connect::poll::graph::GraphBuilder;
use crate::connect::poll::queue::PollQueue;
use crate::connect::poll::traits::AsyncParent;
use crate::connect::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::error::Error;
use crate::node::line::poll::routine::tests::{MockLine, noop_local_waker};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
struct TestThread;
impl ThreadId for TestThread {}

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
    type SignalType = &'static str;
    type ThreadIdType = TestThread;

    fn build(
        self: Box<Self>,
        _edge: Rc<RefCell<PollEdge<usize, &'static str>>>,
        allocator: ThreadLocalWakerAllocator<TestThread>,
    ) -> Result<crate::connect::poll::graph::Graph<'static, TestThread>, Error> {
        Ok(crate::connect::poll::graph::Graph {
            allocator,
            nodes: vec![],
        })
    }
}

macro_rules! deferred {
    ($alloc:expr) => {
        Node::<_, _, usize, usize, &str, TestThread, _>::deferred(mock_factory(), $alloc)
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
    let edge = Rc::new(RefCell::new(PollEdge::new(noop_local_waker())));
    let graph = AsyncParent::build(Box::new(node), edge, local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
}

#[test]
fn async_deferred_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc).parent(MockParent);
    let local = to_local(alloc);
    let edge = Rc::new(RefCell::new(PollEdge::new(noop_local_waker())));
    let graph = AsyncParent::build(Box::new(node), edge, local).unwrap();
    assert_eq!(graph.nodes.len(), 3);
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
