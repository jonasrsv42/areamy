//! Lock-free MPSC stack — a Treiber stack with take-all, over a node per graph id.
//!
//! # Wake correctness
//!
//! A wake that finds its node queued pushes nothing, so the poll it joins must see its writes;
//! a wake landing while the consumer parks must wake it.
//!
//! - The queued state changes by RMW only. The consumer's `release` acquires every claim that
//!   coalesced before it, so the poll after it sees their writes; a claim after it finds the node
//!   idle and pushes it again. A failed-CAS claim gives the `release` nothing to acquire; a
//!   plain-store `release` acquires nothing.
//! - The consumer reads a node's link, then releases it, then polls it.
//! - The consumer parks only after a take found the stack empty.
//!
//! Breaking any loses wakes.
//!
//! A wake that coalesces into a node claimed but not yet published is served once that push
//! publishes: on a priority scheduler, at the claiming producer's priority.

mod link;
mod node;
#[cfg(test)]
mod tests;

pub(super) use link::Link;

use crate::poll::graph::NodeId;
use link::AtomicLink;
use node::{Node, Nodes, State};

use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

pub(super) struct TreiberStack {
    /// The newest pushed id.
    head: AtomicLink,
    /// The arena, made at init.
    nodes: OnceLock<Nodes>,
    /// Orders a push that finds no nodes against `init`. Only RMWs touch it, so they form one
    /// release sequence.
    ///
    /// Such a push is dropped: the caller pushes every id after `init`. Sound only if that first
    /// poll sees what the waker wrote. A look at `nodes` that finds none synchronizes with
    /// nothing, so the push does an RMW here and looks again, and `init` swaps it after making
    /// the nodes. The push's RMW came first: the swap acquires its writes, ahead of the caller's
    /// pushes. The swap came first: the push acquires the nodes.
    early: AtomicUsize,
}

impl TreiberStack {
    /// O(1): no allocation.
    pub(super) fn new() -> Self {
        Self {
            head: AtomicLink::new(Link::NONE),
            nodes: OnceLock::new(),
            early: AtomicUsize::new(0),
        }
    }

    /// Makes the nodes for ids `0..n`. Poll thread, once per graph. O(n): one allocation.
    pub(super) fn init(&self, n: usize) {
        self.nodes.get_or_init(|| Nodes::new(n));
        // AcqRel: earlier early pushes' writes reach the first batch; later ones find the nodes.
        // See `early`.
        self.early.swap(0, Ordering::AcqRel);
    }

    /// Pushes `id` unless queued, from any thread. O(1): a claim; an owning one adds a load, a
    /// store and a CAS.
    #[inline]
    pub(super) fn push(&self, id: NodeId) {
        match self.nodes.get() {
            Some(nodes) => self.push_onto(nodes, id),
            None => self.push_early(id),
        }
    }

    /// A push before `init`: dropped, unless init lands during it.
    fn push_early(&self, id: NodeId) {
        // AcqRel: ordered against init's swap; see `early`.
        self.early.fetch_or(1, Ordering::AcqRel);
        if let Some(nodes) = self.nodes.get() {
            self.push_onto(nodes, id);
        }
    }

    #[inline]
    fn push_onto(&self, nodes: &Nodes, id: NodeId) {
        let Some(node) = nodes.get(id) else {
            #[cfg(not(feature = "silent"))]
            past_the_graph(id, self);
            return;
        };
        match node.claim() {
            // Queued: its coming poll serves this wake.
            State::Queued => {}
            // Idle: this push owns the node until it publishes it.
            State::Idle => self.publish(node, id),
        }
    }

    /// Links the owned `node` on top. O(1): a load, a store and a CAS, and a store and a CAS per
    /// retry.
    #[inline]
    fn publish(&self, node: &Node, id: NodeId) {
        let mut below = self.head.load(Ordering::Relaxed);
        // CAS loop, try to commit the new node to our stack.
        loop {
            // Point at the top we saw; ours alone until published.
            node.link(below);
            // Release: the take sees the link and the waker's writes.
            match self.head.compare_exchange_weak(
                below,
                Link::of(id),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                // Another push or the take moved `head` since our read: relink to the new top and
                // retry. Bounded by the pushes that land meanwhile: each owns its id until the
                // take, so at most about n per batch.
                Err(current) => below = current,
            }
        }
    }

    /// Takes the stack: the batch, from its newest id. Poll thread only. O(1): one swap.
    #[inline]
    pub(super) fn take(&self) -> Link {
        // Take-all: one swap per batch, no ABA. Acquire: pairs with every push's Release.
        self.head.swap(Link::NONE, Ordering::Acquire)
    }

    /// The batch's next id, released; `walk` moves below it. Poll thread only. O(1): a load and a
    /// swap.
    ///
    /// LIFO: the stack's own order, walked in place.
    #[inline]
    pub(super) fn next(&self, walk: &mut Link) -> Option<NodeId> {
        let id = walk.index();
        let node = self.nodes.get()?.get(id)?;
        *walk = node.release();
        Some(id)
    }
}

/// Prints a push past the graph. Cold and out of line, so the push itself needs no stack frame;
/// `id` first, so the push's registers line up with the call.
#[cfg(not(feature = "silent"))]
#[cold]
#[inline(never)]
fn past_the_graph(id: NodeId, stack: &TreiberStack) {
    let n = stack.nodes.get().map_or(0, Nodes::len);
    eprintln!("woke id {id}, past the graph of {n} ids");
}
