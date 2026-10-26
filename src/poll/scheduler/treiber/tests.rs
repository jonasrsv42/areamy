use super::*;
use crate::tests::support::{DEADLINE, allocations, deadline, live_bytes};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::AtomicBool;
use core::{hint, iter, mem};
use std::sync::Barrier;
use std::thread;

fn started(n: usize) -> TreiberStack {
    let stack = TreiberStack::new();
    stack.init(n);
    stack
}

/// The rest of the batch at `walk`, each id released. Capped one past the nodes, so a cycle fails
/// the test instead of hanging it.
fn rest(stack: &TreiberStack, walk: &mut Link) -> Vec<NodeId> {
    let cap = stack.nodes.get().map_or(0, Nodes::len) + 1;
    iter::from_fn(|| stack.next(walk)).take(cap).collect()
}

fn batch(stack: &TreiberStack) -> Vec<NodeId> {
    let mut walk = stack.take();
    rest(stack, &mut walk)
}

/// Seeded xorshift.
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

// ---- batches ----

#[test]
fn a_batch_is_every_push_once_newest_first() {
    let stack = started(8);
    stack.push(5);
    stack.push(3);
    stack.push(5);
    stack.push(7);
    assert_eq!(batch(&stack), [7, 3, 5]);
    assert!(batch(&stack).is_empty());
    stack.push(5);
    assert_eq!(batch(&stack), [5]);
}

/// The claim/release handshake, step by step.
#[test]
fn a_push_coalesces_until_the_release_and_claims_after_it() {
    let stack = started(4);
    stack.push(1);
    stack.push(2);
    // Queued in the stack.
    stack.push(1);
    let mut walk = stack.take();
    // Taken, not yet released.
    stack.push(1);
    assert_eq!(stack.take(), Link::NONE);
    assert_eq!(stack.next(&mut walk), Some(2));
    assert_eq!(stack.next(&mut walk), Some(1));
    // Released: a wake during its poll pushes it anew.
    stack.push(1);
    assert_eq!(stack.next(&mut walk), None);
    assert_eq!(batch(&stack), [1]);
}

/// The walk goes on from the link read before the release.
#[test]
fn a_node_pushed_again_mid_batch_keeps_the_batch_whole() {
    let stack = started(4);
    for id in 0..4 {
        stack.push(id);
    }
    let mut walk = stack.take();
    assert_eq!(stack.next(&mut walk), Some(3));
    stack.push(3);
    stack.push(0);
    assert_eq!(rest(&stack, &mut walk), [2, 1, 0]);
    assert_eq!(batch(&stack), [3]);
}

#[test]
fn an_id_waking_itself_every_poll_starves_nobody() {
    let stack = started(3);
    for id in 0..3 {
        stack.push(id);
    }
    let mut polled = Vec::new();
    for _ in 0..3 {
        for id in batch(&stack) {
            if id == 0 {
                stack.push(0);
            }
            polled.push(id);
        }
    }
    assert_eq!(polled, [2, 1, 0, 0, 0]);
}

/// Random pushes, takes and walk steps against a model: the queued ids, the stack oldest first,
/// and the batch being walked.
#[test]
fn pushes_and_walks_match_a_model() {
    let n = 200;
    let stack = started(n);
    let mut queued = vec![false; n];
    let mut pushed: Vec<NodeId> = Vec::new();
    // Popped from the back: newest first.
    let mut walking: Vec<NodeId> = Vec::new();
    let mut walk = Link::NONE;
    let mut rng = Rng(5);
    for step in 0..40_000 {
        if rng.below(3) == 0 {
            // Mostly low ids, so pushes often meet queued ones.
            let span = 1 + rng.below(n);
            let id = rng.below(span);
            stack.push(id);
            if !queued[id] {
                queued[id] = true;
                pushed.push(id);
            }
        } else if walking.is_empty() {
            walk = stack.take();
            let top = pushed.last().map_or(Link::NONE, |&id| Link::of(id));
            assert_eq!(walk, top, "step {step}");
            walking = mem::take(&mut pushed);
        } else {
            let id = walking.pop();
            assert_eq!(stack.next(&mut walk), id, "step {step}");
            if let Some(id) = id {
                queued[id] = false;
            }
        }
    }
}

/// A link to an id without a node is a stack bug: debug builds catch it.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "batch links id 9, past the graph")]
fn a_link_past_the_graph_fails_debug_builds() {
    let stack = started(4);
    stack.next(&mut Link::of(9));
}

// ---- the claim/release race ----

/// Store-buffer litmus on the claim and release: the consumer releases a queued node, then reads
/// its data; a producer writes the data, then pushes. A poll that read old data while the push
/// coalesced is a lost wake. Detects a non-RMW release only in optimized builds.
#[test]
fn a_push_racing_the_release_never_loses_its_wake() {
    let _deadline = deadline(DEADLINE);
    const N: usize = 1 << 10;
    const CHUNKS: usize = 1 << 10;
    fn cells() -> Arc<Vec<AtomicUsize>> {
        Arc::new((0..N).map(|_| AtomicUsize::new(0)).collect())
    }
    let stack = Arc::new(started(N));
    let (data, seen) = (cells(), cells());
    let go = Arc::new(Barrier::new(3));
    let consumer = thread::spawn({
        let (stack, data, seen, go) = (stack.clone(), data.clone(), seen.clone(), go.clone());
        move || {
            for _ in 0..CHUNKS {
                go.wait();
                let mut walk = stack.take();
                while let Some(id) = stack.next(&mut walk) {
                    seen[id].store(data[id].load(Ordering::Relaxed), Ordering::Relaxed);
                }
                go.wait();
            }
        }
    });
    let producer = thread::spawn({
        let (stack, data, go) = (stack.clone(), data.clone(), go.clone());
        move || {
            for _ in 0..CHUNKS {
                go.wait();
                // The consumer's order.
                for id in (0..N).rev() {
                    data[id].store(1, Ordering::Relaxed);
                    stack.push(id);
                }
                go.wait();
            }
        }
    });
    let mut lost = 0;
    for _ in 0..CHUNKS {
        for (id, data) in data.iter().enumerate() {
            data.store(0, Ordering::Relaxed);
            stack.push(id);
        }
        go.wait();
        go.wait();
        // Pushed after their release: served by the next batch.
        let mut again = vec![false; N];
        for id in batch(&stack) {
            again[id] = true;
        }
        lost += (0..N)
            .filter(|&id| seen[id].load(Ordering::Relaxed) == 0 && !again[id])
            .count();
    }
    consumer.join().unwrap();
    producer.join().unwrap();
    assert_eq!(lost, 0);
}

// ---- init ----

#[test]
fn init_pushes_nothing_itself() {
    let stack = started(4);
    assert!(batch(&stack).is_empty());
    stack.push(3);
    assert_eq!(batch(&stack), [3]);
}

/// `n` is the first id past the graph.
#[test]
fn a_push_past_the_graph_is_dropped() {
    let stack = started(4);
    stack.push(4);
    assert!(batch(&stack).is_empty());
}

#[test]
fn a_push_before_init_pushes_nothing() {
    let stack = TreiberStack::new();
    stack.push(1);
    assert!(batch(&stack).is_empty());
    stack.init(4);
    assert!(batch(&stack).is_empty());
    stack.push(1);
    assert_eq!(batch(&stack), [1]);
}

/// Init lands between the push's look for the nodes and its RMW.
#[test]
fn a_push_that_missed_the_nodes_finds_them_after_its_rmw() {
    let stack = TreiberStack::new();
    stack.init(4);
    stack.push_early(2);
    assert_eq!(batch(&stack), [2]);
}

/// Init lands after the push's RMW.
#[test]
fn a_push_whose_rmw_beat_init_is_dropped() {
    let stack = TreiberStack::new();
    stack.push_early(2);
    stack.init(4);
    assert!(batch(&stack).is_empty());
}

/// Wakers push from before init until after it, init coming later each round: the push each
/// makes once it has seen init lands, and nothing else is pushed.
#[test]
fn pushes_racing_init_all_land_once_init_is_seen() {
    let _deadline = deadline(DEADLINE);
    let n = 133;
    let pushed = [0, 1, 63, 65, n - 1];
    for round in 0..20 {
        let stack = Arc::new(TreiberStack::new());
        let seen = Arc::new(AtomicBool::new(false));
        let go = Arc::new(Barrier::new(pushed.len() + 1));
        let wakers: Vec<_> = pushed
            .iter()
            .map(|&id| {
                let (stack, seen, go) = (stack.clone(), seen.clone(), go.clone());
                thread::spawn(move || {
                    go.wait();
                    loop {
                        stack.push(id);
                        if seen.load(Ordering::SeqCst) {
                            stack.push(id);
                            break;
                        }
                        // More wakers than cores would starve init.
                        thread::yield_now();
                    }
                })
            })
            .collect();
        go.wait();
        // Busy, so the delay doesn't wait on the wakers' turns.
        for _ in 0..round * 200 {
            hint::spin_loop();
        }
        stack.init(n);
        seen.store(true, Ordering::SeqCst);
        for waker in wakers {
            waker.join().unwrap();
        }
        let mut landed = batch(&stack);
        landed.sort_unstable();
        assert_eq!(landed, pushed, "round {round}");
    }
}

// ---- memory ----

#[test]
fn the_nodes_are_one_allocation_made_at_init() {
    let before = live_bytes();
    let stack = TreiberStack::new();
    let made = allocations();
    stack.init(4_000);
    assert_eq!(allocations() - made, 1);
    stack.push(3_999);
    assert_eq!(batch(&stack), [3_999]);
    drop(stack);
    assert_eq!(live_bytes(), before);
}
