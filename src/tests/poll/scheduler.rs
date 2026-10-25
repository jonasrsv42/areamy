//! The scheduler through the real poll loop: remote wakes past `usize::BITS` nodes, and a runtime
//! nested in another's poll.

use crate::edge::sync::tests::wait_until;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::poll;
use crate::poll::graph::{Graph, GraphBuilder, GraphNode};
use crate::poll::marker::NodeId;
use crate::poll::wakers::ThreadLocalWakerAllocator;
use crate::poll::{Context, Pollable};
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::Join;
use crate::wake::LocalWaker;
use alloc::sync::Arc;
use core::cell::{Cell, RefCell};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::Poll;
use core::time::Duration;
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use super::mock::IoThread;

crate::thread_id!(OuterThread, InnerThread);

/// Ids per machine word: the graph here reaches past it.
const BITS: usize = usize::BITS as usize;

/// Builds one node from `P`, on a slot of its own.
struct One<P>(P);

impl<P> GraphBuilder<'static, P::ThreadId> for One<P>
where
    P: Pollable + Send + 'static,
{
    fn build(
        self: Box<Self>,
        mut allocator: ThreadLocalWakerAllocator<P::ThreadId>,
    ) -> Result<Graph<'static, P::ThreadId>, Error> {
        let slot = allocator.next();
        let mut graph = Graph::empty(allocator);
        graph.nodes.push(GraphNode {
            id: slot.id,
            pollable: Box::new(self.0),
        });
        Ok(graph)
    }
}

// ---- remote wakes past `BITS` nodes ----

/// Counts its polls. Node `dead` ends on its first; the others once `stop` is set.
struct Counted {
    id: NodeId,
    dead: NodeId,
    stop: Arc<AtomicBool>,
    polls: Arc<Vec<AtomicUsize>>,
}

impl Connection for Counted {}

impl Pollable for Counted {
    type ThreadId = IoThread;

    fn poll(&mut self, _cx: &mut Context) -> Result<Poll<()>, Error> {
        self.polls[self.id].fetch_add(1, Ordering::SeqCst);
        if self.id == self.dead || self.stop.load(Ordering::SeqCst) {
            return Ok(Poll::Ready(()));
        }
        Ok(Poll::Pending)
    }
}

/// A [Counted] node on every slot allocated before the thread started.
struct Counting {
    dead: NodeId,
    stop: Arc<AtomicBool>,
    polls: Arc<Vec<AtomicUsize>>,
}

impl GraphBuilder<'static, IoThread> for Counting {
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<IoThread>,
    ) -> Result<Graph<'static, IoThread>, Error> {
        let nodes = (0..self.polls.len())
            .map(|id| GraphNode {
                id,
                pollable: Box::new(Counted {
                    id,
                    dead: self.dead,
                    stop: self.stop.clone(),
                    polls: self.polls.clone(),
                }) as Box<dyn Pollable<ThreadId = IoThread>>,
            })
            .collect();
        Ok(Graph { allocator, nodes })
    }
}

/// Past `BITS` nodes, a remote wake polls only its own node: the others, `BITS` ids apart from it
/// included, poll once at the start and once to stop. A node that ended on its first poll is
/// skipped however often it is woken, and the thread still ends cleanly.
#[test]
fn a_remote_wake_past_bits_polls_only_its_node() {
    let _deadline = deadline(DEADLINE);
    let len = BITS + 7;
    let (dead, hot) = (5, BITS + 5);
    let stop = Arc::new(AtomicBool::new(false));
    let polls: Arc<Vec<AtomicUsize>> = Arc::new((0..len).map(|_| AtomicUsize::new(0)).collect());
    let mut poll_thread = poll::Thread::<IoThread>::new();
    // A cross-thread waker per node, allocated before the thread exists.
    let wakers: Vec<_> = (0..len)
        .map(|_| poll_thread.waker_allocator().next().value)
        .collect();
    poll_thread.add(Counting {
        dead,
        stop: stop.clone(),
        polls: polls.clone(),
    });
    thread::scope(|s| {
        let handle = poll_thread.start(s);
        // `dead` ends in its first poll.
        wait_until(|| polls[dead].load(Ordering::SeqCst) > 0);
        for round in 0..2_000 {
            wakers[hot].wake_by_ref();
            wakers[dead].wake_by_ref();
            // Now and then let the thread park, so wakes find it asleep too.
            if round % 64 == 0 {
                thread::sleep(Duration::from_micros(200));
            }
        }
        wait_until(|| polls[hot].load(Ordering::SeqCst) >= 2);
        // Every node but `hot` is still at its first poll.
        for (id, polls) in polls.iter().enumerate() {
            if id != hot {
                assert_eq!(
                    polls.load(Ordering::SeqCst),
                    1,
                    "{id} polled without a wake"
                );
            }
        }
        stop.store(true, Ordering::SeqCst);
        for waker in &wakers {
            waker.wake_by_ref();
        }
        assert!(matches!(handle.join(), Join::Ok));
    });
    assert_eq!(polls[dead].load(Ordering::SeqCst), 1);
    for (id, polls) in polls.iter().enumerate() {
        if id != dead && id != hot {
            assert_eq!(
                polls.load(Ordering::SeqCst),
                2,
                "{id}: first poll, stop poll"
            );
        }
    }
}

// ---- a runtime nested in another's poll ----

thread_local! {
    /// The outer node's local waker, for the inner node to wake.
    static OUTER: RefCell<Option<LocalWaker>> = const { RefCell::new(None) };
    /// Whether the inner node's local waker `will_wake` the outer node's.
    static SAME: Cell<Option<bool>> = const { Cell::new(None) };
}

/// The inner runtime's only node: wakes itself, then waits on a timer, then wakes the outer node.
struct Inner {
    polls: u8,
}

impl Connection for Inner {}

impl Pollable for Inner {
    type ThreadId = InnerThread;

    fn poll(&mut self, cx: &mut Context) -> Result<Poll<()>, Error> {
        self.polls += 1;
        match self.polls {
            1 => cx.wake.local.wake(),
            2 => {
                let due = Instant::now() + Duration::from_millis(5);
                let _ = cx.timer.schedule_at(due);
            }
            _ => {
                OUTER.with_borrow(|outer| {
                    let outer = outer.as_ref().unwrap();
                    SAME.set(Some(cx.wake.local.will_wake(outer)));
                    outer.wake();
                });
                return Ok(Poll::Ready(()));
            }
        }
        Ok(Poll::Pending)
    }
}

/// The outer runtime's only node: runs the inner runtime to completion inside its first poll,
/// then ends when polled again.
struct Outer {
    ran: bool,
    same: Arc<Mutex<Option<bool>>>,
}

impl Connection for Outer {}

impl Pollable for Outer {
    type ThreadId = OuterThread;

    fn poll(&mut self, cx: &mut Context) -> Result<Poll<()>, Error> {
        if self.ran {
            *self.same.lock().unwrap() = SAME.get();
            return Ok(Poll::Ready(()));
        }
        self.ran = true;
        OUTER.set(Some(cx.wake.local.clone()));
        let mut inner = poll::Thread::<InnerThread>::new();
        inner.add(One(Inner { polls: 0 }));
        inner.run()?;
        Ok(Poll::Pending)
    }
}

/// Both runtimes number their node 0 on one thread, yet keep separate stacks: the inner node's
/// waker is not the outer's, and its wake of the outer node, made inside the outer node's poll,
/// polls the outer node again.
#[test]
fn a_nested_runtime_wakes_the_outer_node() {
    let _deadline = deadline(DEADLINE);
    let same = Arc::new(Mutex::new(None));
    let mut outer = poll::Thread::<OuterThread>::new();
    outer.add(One(Outer {
        ran: false,
        same: same.clone(),
    }));
    thread::scope(|s| assert!(matches!(outer.start(s).join(), Join::Ok)));
    assert_eq!(*same.lock().unwrap(), Some(false));
}
