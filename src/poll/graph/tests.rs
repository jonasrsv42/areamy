use super::*;
use crate::graph::marker::Connection;
use crate::poll::Context;
use crate::tests::support::live_bytes;

use core::task::Poll;

crate::thread_id!(GraphThread);

struct Idle;

impl Connection for Idle {}

impl Pollable for Idle {
    type ThreadId = GraphThread;
    fn poll(&mut self, _cx: &mut Context) -> Result<Poll<()>, Error> {
        Ok(Poll::Pending)
    }
}

fn fails_with(graph: Graph<'static, GraphThread>, message: &str) {
    match graph.finish() {
        Err(e) => assert!(e.to_string().contains(message), "{e}"),
        Ok(_) => panic!("finished, expected {message:?}"),
    }
}

#[test]
fn the_runtime_polls_with_the_wakers_handed_out() {
    let mut reservations = Reservations::<GraphThread>::new();
    let reserved = reservations.next();
    let early = reserved.waker().clone();
    let mut graph = reservations.local();
    let slot = graph.claim(reserved);
    let late = graph.next();
    let late_cx = late.cx().clone();
    graph.add(slot, Idle);
    graph.add(late, Idle);
    let (runtime, _) = graph.finish().unwrap();
    let cx = |id: NodeId| &runtime.nodes[id].as_ref().expect("added").cx;
    assert!(early.will_wake(&cx(0).wake.sync));
    assert!(late_cx.wake.sync.will_wake(&cx(1).wake.sync));
    assert!(late_cx.wake.local.will_wake(&cx(1).wake.local));
    assert!(late_cx.timer.same_target(&cx(1).timer));
    assert!(!cx(0).timer.same_target(&cx(1).timer));
}

/// As the poll thread runs it: every node polls once first, in id order, whatever order they
/// were added in.
#[test]
fn finish_marks_every_node_for_a_first_poll_in_id_order() {
    let mut reservations = Reservations::<GraphThread>::new();
    let reserved = reservations.next();
    let mut graph = reservations.local();
    let late = graph.next();
    graph.add(late, Idle);
    let slot = graph.claim(reserved);
    graph.add(slot, Idle);
    let (runtime, mut consumer) = graph.finish().unwrap();
    assert_eq!(runtime.nodes.len(), 2);
    assert_eq!(consumer.woken(), [0, 1]);
}

#[test]
fn a_node_reserved_but_never_added_fails_the_build() {
    let mut reservations = Reservations::<GraphThread>::new();
    let _orphan = reservations.next();
    let mut graph = reservations.local();
    let slot = graph.next();
    graph.add(slot, Idle);
    fails_with(graph, "slot 0 is empty");
}

#[test]
fn a_slot_made_but_never_added_fails_the_build() {
    let mut graph = Reservations::<GraphThread>::new().local();
    let slot = graph.next();
    graph.add(slot, Idle);
    let _orphan = graph.next();
    fails_with(graph, "slot 1 is empty");
}

/// A graph's build, finished and dropped, frees all it made.
#[test]
fn a_graph_frees_all_it_made() {
    let life = || {
        let mut reservations = Reservations::<GraphThread>::new();
        let reserved = reservations.next();
        let mut graph = reservations.local();
        let slot = graph.claim(reserved);
        graph.add(slot, Idle);
        let slot = graph.next();
        graph.add(slot, Idle);
        drop(graph.finish().unwrap());
    };
    life();
    let before = live_bytes();
    life();
    assert_eq!(live_bytes(), before);
}
