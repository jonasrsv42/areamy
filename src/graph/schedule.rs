use crate::graph::marker::{Connection, Multiplicity, Unary};
use crate::wake;

mod default;

/// A [`Schedule`] connection takes a waker to wake at some future point. When is up to the
/// implementation and the [Multiplicity] marker; each implementation states it. Not the wiring
/// verb [crate::Schedules].
///
/// Bound once, not passed per refusal: there is no try-then-register race, and a wake only means
/// "retry". A concrete [wake::Waker]: not generic over the waker (a node behind a dyn sink
/// can't pick one), not a poll `Context` (no timers in `graph`).
///
/// [Schedule] is generic over [Multiplicity] to allow one [Connection] to schedule several
/// semantically different things. Per default all implementations are [Unary] unless otherwise
/// stated to avoid specifying [Multiplicity] where not necessary.
pub trait Schedule<MultiplicityType: Multiplicity = Unary>: Connection {
    /// Schedule `waker`. By value: the connection keeps it, so the caller decides whether to
    /// clone.
    fn schedule(&mut self, waker: wake::Waker);
}
