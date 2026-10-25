//! [Link]: an id or none, in one word.

use crate::poll::marker::NodeId;

use core::sync::atomic::{AtomicUsize, Ordering};

/// An id, or none: the stack's bottom, a batch's end. None is `usize::MAX`, an index past every
/// node.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::poll::scheduler) struct Link(usize);

impl Link {
    pub(in crate::poll::scheduler) const NONE: Self = Self(usize::MAX);

    #[inline]
    pub(super) fn of(id: NodeId) -> Self {
        Self(id)
    }

    /// The node to read.
    #[inline]
    pub(super) fn index(self) -> usize {
        self.0
    }
}

/// A [Link] any thread reads and writes.
pub(super) struct AtomicLink(AtomicUsize);

impl AtomicLink {
    pub(super) const fn new(link: Link) -> Self {
        Self(AtomicUsize::new(link.0))
    }

    #[inline]
    pub(super) fn load(&self, order: Ordering) -> Link {
        Link(self.0.load(order))
    }

    #[inline]
    pub(super) fn store(&self, link: Link, order: Ordering) {
        self.0.store(link.0, order);
    }

    #[inline]
    pub(super) fn swap(&self, link: Link, order: Ordering) -> Link {
        Link(self.0.swap(link.0, order))
    }

    #[inline]
    pub(super) fn compare_exchange_weak(
        &self,
        current: Link,
        new: Link,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Link, Link> {
        self.0
            .compare_exchange_weak(current.0, new.0, success, failure)
            .map(Link)
            .map_err(Link)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_indexes_its_node_and_none_indexes_past_every_node() {
        for id in [0, 1, 4_095] {
            assert_eq!(Link::of(id).index(), id);
            assert_ne!(Link::of(id), Link::NONE);
        }
        assert_eq!(Link::NONE.index(), usize::MAX);
    }

    #[test]
    fn an_atomic_link_holds_the_last_link_written() {
        let link = AtomicLink::new(Link::NONE);
        assert_eq!(link.load(Ordering::SeqCst), Link::NONE);
        link.store(Link::of(3), Ordering::SeqCst);
        assert_eq!(link.swap(Link::of(4), Ordering::SeqCst), Link::of(3));
        assert_eq!(
            link.compare_exchange_weak(Link::of(3), Link::NONE, Ordering::SeqCst, Ordering::SeqCst),
            Err(Link::of(4))
        );
        // Weak: retried past spurious failures.
        while link
            .compare_exchange_weak(Link::of(4), Link::NONE, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {}
        assert_eq!(link.load(Ordering::SeqCst), Link::NONE);
    }
}
