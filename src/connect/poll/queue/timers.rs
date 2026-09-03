//! Timer identity for the deadline heap.

/// Index into the deadline heap's slot table. Transparent newtype so
/// slot indices can't be mixed up with node ids or other counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SlotId(pub(crate) usize);

impl SlotId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

/// Slot reuse counter — the ABA guard for recycled slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Generation(u64);

impl Generation {
    pub(crate) fn first() -> Self {
        Self(0)
    }

    /// Successor generation. Wraps on overflow rather than panic: a
    /// key surviving a full 2^64 cycle of its slot would be falsely
    /// live — astronomically unlikely.
    pub(crate) fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

/// Handle to one live timer, returned by `schedule_at`. Cancelling or
/// firing invalidates the key — later use is a no-op (ABA-guarded by
/// [Self::generation]). Fields are crate-private — only the deadline
/// heap mints these.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerKey {
    pub(crate) slot: SlotId,
    pub(crate) generation: Generation,
}
