//! Builder tests: GraphBuilder::build, AsyncParent::build, and typestate transitions.

use super::node::Node;
use crate::ThreadId;
use crate::connect::poll::edge::PollEdge;
use crate::connect::poll::graph::GraphBuilder;
use crate::connect::poll::queue::PollQueue;
use crate::connect::poll::traits::AsyncParent;
use crate::connect::poll::wakers::{ThreadLocalWakerAllocator, WakerAllocator};
use crate::error::Error;
use crate::node::biunion::poll::routine::tests::{MockBiunion, noop_local_waker};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
struct TestThread;
impl ThreadId for TestThread {}

fn mock_factory() -> impl FnOnce(crate::connect::waker::ThreadLocalWaker) -> MockBiunion + Send {
    |waker| MockBiunion::new(waker)
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

/// Macro to create a deferred biunion node with correct type annotation.
macro_rules! deferred {
    ($alloc:expr) => {
        Node::<_, _, _, _, usize, usize, usize, &str, TestThread, _>::deferred(
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
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .input::<crate::biunion::Right, crate::poll::Sync>()
        .output::<crate::poll::Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_sync_deferred_builds_four_nodes() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .input::<crate::biunion::Right, crate::poll::Sync>();
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
        .parent::<crate::biunion::Left>(MockParent)
        .input::<crate::biunion::Right, crate::poll::Sync>()
        .output::<crate::poll::Sync>();
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
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .parent::<crate::biunion::Right>(MockParent)
        .output::<crate::poll::Sync>();
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
        .parent::<crate::biunion::Left>(MockParent)
        .parent::<crate::biunion::Right>(MockParent)
        .output::<crate::poll::Sync>();
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
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .input::<crate::biunion::Right, crate::poll::Sync>();
    let local = to_local(alloc);
    let edge = Rc::new(RefCell::new(PollEdge::new(noop_local_waker())));
    let graph = AsyncParent::build(Box::new(node), edge, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn async_async_deferred_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<crate::biunion::Left>(MockParent)
        .parent::<crate::biunion::Right>(MockParent);
    let local = to_local(alloc);
    let edge = Rc::new(RefCell::new(PollEdge::new(noop_local_waker())));
    let graph = AsyncParent::build(Box::new(node), edge, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// Builder order — right resolved before left
// ============================================================

#[test]
fn right_async_then_left_sync() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<crate::biunion::Right>(MockParent)
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .output::<crate::poll::Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn right_async_then_left_async() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<crate::biunion::Right>(MockParent)
        .parent::<crate::biunion::Left>(MockParent)
        .output::<crate::poll::Sync>();
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
        .parent::<crate::biunion::Left>(MockParent)
        .input::<crate::biunion::Right, crate::poll::Sync>();
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_left_async_right_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .parent::<crate::biunion::Right>(MockParent);
    let local = to_local(alloc);
    let graph = GraphBuilder::build(Box::new(node), local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn async_async_deferred_sink() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .parent::<crate::biunion::Left>(MockParent)
        .parent::<crate::biunion::Right>(MockParent);
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
        .parent::<crate::biunion::Left>(MockParent)
        .input::<crate::biunion::Right, crate::poll::Sync>();
    let local = to_local(alloc);
    let edge = Rc::new(RefCell::new(PollEdge::new(noop_local_waker())));
    let graph = AsyncParent::build(Box::new(node), edge, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

#[test]
fn sync_left_async_right_as_parent() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .parent::<crate::biunion::Right>(MockParent);
    let local = to_local(alloc);
    let edge = Rc::new(RefCell::new(PollEdge::new(noop_local_waker())));
    let graph = AsyncParent::build(Box::new(node), edge, local).unwrap();
    assert_eq!(graph.nodes.len(), 4);
}

// ============================================================
// Node IDs are unique
// ============================================================

#[test]
fn node_ids_are_unique() {
    let (mut alloc, to_local) = make_allocators();
    let node = deferred!(&mut alloc)
        .input::<crate::biunion::Left, crate::poll::Sync>()
        .input::<crate::biunion::Right, crate::poll::Sync>()
        .output::<crate::poll::Sync>();
    let local = to_local(alloc);
    let graph = Box::new(node).build(local).unwrap();
    let mut ids: Vec<_> = graph.nodes.iter().map(|n| n.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4);
}
