//! Poll node options set through `thread.builder()`: input bounds per edge, per side.

use crate::biunion::{Left, Right};
use crate::edge::sync::tests::join_within;
use crate::error::Error;
use crate::graph::{Get, TryPush, TryPushable};
use crate::message::Message;
use crate::node::biunion::poll::routine::tests::MockBiunion;
use crate::node::line::poll::routine::tests::MockLine;
use crate::poll::graph::Graph;
use crate::poll::wakers::ThreadLocalWakerAllocator;
use crate::poll::{self, AsyncParent};
use crate::signal::Trackable;
use crate::work;
use crate::{At, Via};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::thread;

crate::thread_id!(IoThread);

type Signal = Trackable<&'static str>;
type WorkSink = dyn work::Sink<DataType = usize, SignalType = Signal> + Send + std::marker::Sync;

/// Whether each of `count` try_pushes went in.
fn accepted(sink: &mut Box<WorkSink>, count: usize) -> Vec<bool> {
    (0..count)
        .map(|value| matches!(sink.try_push(Message::Data(value)), Ok(TryPush::Pushed)))
        .collect()
}

#[test]
fn a_bounded_line_bridge_refuses_past_its_bound() {
    let mut thread = poll::Thread::<IoThread>::new();
    let node = thread
        .builder()
        .line()
        .bounded(NonZeroUsize::MIN)
        .build(MockLine::new)
        .input()
        .output();
    let mut sink: Box<WorkSink> = Get::get(&node).unwrap();
    assert_eq!(accepted(&mut sink, 2), [true, false]);
}

/// `None`, like no `.bounded` at all, is unbounded.
#[test]
fn an_unbounded_line_bridge_takes_everything() {
    let mut thread = poll::Thread::<IoThread>::new();
    let node = thread
        .builder()
        .line()
        .bounded(None)
        .build(MockLine::new)
        .input()
        .output();
    let mut sink: Box<WorkSink> = Get::get(&node).unwrap();
    assert!(accepted(&mut sink, 100).iter().all(|&pushed| pushed));
}

#[test]
fn a_biunion_bounds_each_side_on_its_own() {
    let mut thread = poll::Thread::<IoThread>::new();
    let node = thread
        .builder()
        .biunion()
        .bounded::<Left>(NonZeroUsize::MIN)
        .build(MockBiunion::new)
        .input::<Left>()
        .input::<Right>()
        .output();
    let mut left: Box<WorkSink> = Get::<WorkSink, Left>::get(&node).unwrap();
    let mut right: Box<WorkSink> = Get::<WorkSink, Right>::get(&node).unwrap();
    assert_eq!(accepted(&mut left, 2), [true, false]);
    assert_eq!(accepted(&mut right, 2), [true, true]);
}

/// Right resolved first: its own bound, and Left's stays its own.
#[test]
fn a_biunion_bounds_the_right_side_resolved_first() {
    let mut thread = poll::Thread::<IoThread>::new();
    let node = thread
        .builder()
        .biunion()
        .bounded::<Right>(NonZeroUsize::MIN)
        .build(MockBiunion::new)
        .input::<Right>()
        .input::<Left>()
        .output();
    let mut left: Box<WorkSink> = Get::<WorkSink, Left>::get(&node).unwrap();
    let mut right: Box<WorkSink> = Get::<WorkSink, Right>::get(&node).unwrap();
    assert_eq!(accepted(&mut left, 2), [true, true]);
    assert_eq!(accepted(&mut right, 2), [true, false]);
}

/// Left by parent, Right by bridge, in both orders: each side keeps its own bound.
#[test]
fn a_biunion_bounds_mixed_inputs_in_either_order() {
    let mut thread = poll::Thread::<IoThread>::new();
    let (parent, parent_seen) = Probe::new();
    let node = thread
        .builder()
        .biunion()
        .bounded::<Right>(NonZeroUsize::MIN)
        .build(MockBiunion::new);
    let node = parent
        .via(node.at::<Left>())
        .unwrap()
        .input::<Right>()
        .output();
    {
        let mut right: Box<WorkSink> = Get::<WorkSink, Right>::get(&node).unwrap();
        assert_eq!(accepted(&mut right, 2), [true, false]);
    }
    thread.add(node);
    run(thread);
    assert_eq!(*parent_seen.lock().unwrap(), [true, true]);

    let mut thread = poll::Thread::<IoThread>::new();
    let (parent, parent_seen) = Probe::new();
    let node = thread
        .builder()
        .biunion()
        .bounded::<Left>(NonZeroUsize::MIN)
        .build(MockBiunion::new)
        .input::<Right>();
    let node = parent.via(node.at::<Left>()).unwrap().output();
    {
        let mut right: Box<WorkSink> = Get::<WorkSink, Right>::get(&node).unwrap();
        assert_eq!(accepted(&mut right, 2), [true, true]);
    }
    thread.add(node);
    run(thread);
    assert_eq!(*parent_seen.lock().unwrap(), [true, false]);
}

/// A parent that tries two pushes into the edge it is built with, and records which went in.
struct Probe(Arc<Mutex<Vec<bool>>>);

impl Probe {
    fn new() -> (Self, Arc<Mutex<Vec<bool>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        (Probe(seen.clone()), seen)
    }
}

impl AsyncParent<'static> for Probe {
    type OutType = usize;
    type SignalType = Signal;
    type ThreadIdType = IoThread;

    fn build(
        self: Box<Self>,
        mut edge: poll::edge::Sender<usize, Signal>,
        allocator: ThreadLocalWakerAllocator<IoThread>,
    ) -> Result<Graph<'static, IoThread>, Error> {
        for value in 0..2 {
            let pushed = matches!(edge.try_push(Message::Data(value))?, TryPush::Pushed);
            self.0.lock().unwrap().push(pushed);
        }
        Ok(Graph {
            allocator,
            nodes: vec![],
        })
    }
}

/// Builds and runs `thread` on its own thread; the probes' edges close as they drop, so it ends.
fn run(thread: poll::Thread<'static, IoThread>) {
    join_within(thread::spawn(move || thread.run())).unwrap();
}

#[test]
fn a_bounded_line_bounds_each_parent_edge() {
    let mut thread = poll::Thread::<IoThread>::new();
    let (first, first_seen) = Probe::new();
    let (second, second_seen) = Probe::new();
    let node = thread
        .builder()
        .line()
        .bounded(NonZeroUsize::MIN)
        .build(MockLine::new);
    let node = first.via(node).unwrap();
    let node = second.via(node).unwrap().output();
    thread.add(node);
    run(thread);
    assert_eq!(*first_seen.lock().unwrap(), [true, false]);
    assert_eq!(*second_seen.lock().unwrap(), [true, false]);
}

#[test]
fn a_biunion_bounds_each_sides_parent_edges() {
    let mut thread = poll::Thread::<IoThread>::new();
    let (left, left_seen) = Probe::new();
    let (right, right_seen) = Probe::new();
    let node = thread
        .builder()
        .biunion()
        .bounded::<Left>(NonZeroUsize::MIN)
        .build(MockBiunion::new);
    let node = left.via(node.at::<Left>()).unwrap();
    let node = right.via(node.at::<Right>()).unwrap().output();
    thread.add(node);
    run(thread);
    assert_eq!(*left_seen.lock().unwrap(), [true, false]);
    assert_eq!(*right_seen.lock().unwrap(), [true, true]);
}
