//! An arena of one [Node] per graph id, allocated once at init, so pushes and takes never
//! allocate.

use super::link::{AtomicLink, Link};

use alloc::boxed::Box;
use core::sync::atomic::{AtomicUsize, Ordering};

/// A node's place in the stack: idle, or queued from a push's claim to the consumer's release.
#[repr(usize)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum State {
    Idle = 0,
    Queued = 1,
}

impl State {
    /// Reads the queued bit only: `claim` sets just it and `release` clears the word, so no other
    /// bit is ever set, and a stray one (a bit flip) is ignored rather than read as queued.
    #[inline]
    fn of(word: usize) -> Self {
        if word & State::Queued as usize != 0 {
            State::Queued
        } else {
            State::Idle
        }
    }
}

/// A [State] in a word: a byte RMW is a masked CAS loop on ESP32-S3.
struct AtomicState(AtomicUsize);

impl AtomicState {
    const fn new() -> Self {
        Self(AtomicUsize::new(State::Idle as usize))
    }

    /// Returns the state it found: `Idle` means this push owns the node. O(1): one `fetch_or`,
    /// a write either way.
    #[inline]
    fn claim(&self) -> State {
        // AcqRel: releases a coalescing waker's writes to the consumer's `release`; acquires that
        // `release`, so the consumer's read of the link comes first.
        State::of(self.0.fetch_or(State::Queued as usize, Ordering::AcqRel))
    }

    /// O(1): one swap.
    #[inline]
    fn release(&self) {
        // AcqRel: acquires every coalescing claim; releases the link read to the next claim.
        self.0.swap(State::Idle as usize, Ordering::AcqRel);
    }
}

pub(super) struct Node {
    state: AtomicState,
    /// The id below while queued. Written by the owning push only, read at the release.
    below: AtomicLink,
}

impl Node {
    #[inline]
    pub(super) fn claim(&self) -> State {
        self.state.claim()
    }

    /// Owning push only, before it publishes. O(1): a store.
    #[inline]
    pub(super) fn link(&self, below: Link) {
        // Relaxed: the publish's Release carries it.
        self.below.store(below, Ordering::Relaxed);
    }

    /// Frees the node for new wakes; returns the id below. Poll thread only. O(1): a load and a
    /// swap.
    #[inline]
    pub(super) fn release(&self) -> Link {
        // Relaxed: the take's Acquire covered the owning push's store. Read first: once released,
        // a push may relink it.
        let below = self.below.load(Ordering::Relaxed);
        self.state.release();
        below
    }
}

pub(super) struct Nodes(Box<[Node]>);

impl Nodes {
    /// All idle. O(n): one allocation.
    pub(super) fn new(n: usize) -> Self {
        Self(
            (0..n)
                .map(|_| Node {
                    state: AtomicState::new(),
                    below: AtomicLink::new(Link::NONE),
                })
                .collect(),
        )
    }

    /// For the out-of-range print.
    #[cfg(any(test, not(feature = "silent")))]
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    /// None past the graph. O(1).
    #[inline]
    pub(super) fn get(&self, index: usize) -> Option<&Node> {
        self.0.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poll::graph::NodeId;

    fn at(nodes: &Nodes, id: NodeId) -> &Node {
        nodes.get(id).expect("an id of the graph")
    }

    #[test]
    fn a_node_is_owned_by_one_claim_until_its_release() {
        let nodes = Nodes::new(2);
        let node = at(&nodes, 1);
        assert_eq!(node.claim(), State::Idle);
        assert_eq!(node.claim(), State::Queued);
        assert_eq!(node.claim(), State::Queued);
        node.release();
        assert_eq!(node.claim(), State::Idle);
        assert_eq!(at(&nodes, 0).claim(), State::Idle);
    }

    #[test]
    fn a_release_hands_back_the_link_below() {
        let nodes = Nodes::new(3);
        let node = at(&nodes, 2);
        assert_eq!(node.claim(), State::Idle);
        assert_eq!(node.release(), Link::NONE);
        assert_eq!(node.claim(), State::Idle);
        node.link(Link::of(1));
        assert_eq!(node.release(), Link::of(1));
    }

    #[test]
    fn an_arena_holds_the_graphs_ids_only() {
        let nodes = Nodes::new(3);
        assert_eq!(nodes.len(), 3);
        assert!(nodes.get(2).is_some());
        assert!(nodes.get(3).is_none());
        assert!(nodes.get(Link::NONE.index()).is_none());
    }
}
