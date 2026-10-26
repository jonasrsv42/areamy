//! [TimerKey]: a live timer's heap cell and the generation that owns it.

/// Index into the deadline heap's cell table. Transparent newtype so
/// cell indices can't be mixed up with node ids or other counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct CellId(pub(crate) usize);

impl CellId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

/// Cell reuse counter — the ABA guard for recycled cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct Generation(u64);

impl Generation {
    pub(crate) fn first() -> Self {
        Self(0)
    }

    /// Successor generation. Wraps on overflow rather than panic: a
    /// key surviving a full 2^64 cycle of its cell would be falsely
    /// live — astronomically unlikely.
    pub(crate) fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

/// A live timer, held by its [TimerGuard](super::TimerGuard). Firing or cancelling invalidates it:
/// later use is a no-op, its generation guarding the cell's reuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerKey {
    pub(crate) cell: CellId,
    pub(crate) generation: Generation,
}
