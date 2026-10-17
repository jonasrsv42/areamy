//! Work graphs: blocking nodes scheduled by their owner (downstream) through [Workable].
//!
//! A work thread follows one call stack, so it waits on one thing at a time: while a node
//! blocks (say a [Line] with no upstream waiting on its [Writer]), its siblings under the same owner
//! don't run, even if they have data. Merge independent sources with [crate::Pushes] side paths
//! into one input, a [Biunion] (its two inputs wait together), or the poll paradigm.
//!
//! A [Biunion] takes left input before right, so a left side that never empties starves the
//! right. Fair scheduling between sources is what the poll paradigm is for.
//!
//! A diamond whose side branch has no upstream of its own (P feeds C by [crate::Via] and D by
//! [crate::Pushes]; D feeds C by [crate::Via]) isn't supported: D blocks waiting for P, which only runs
//! when C schedules it, on the same stack.
//!
//! # Bounded inputs
//!
//! Bound an input only when its producers are the node's own [crate::Via] upstreams or run on
//! other threads. An upstream refused by such an input yields and finds room when rescheduled,
//! because the node drains its input before scheduling its upstreams again. Keep an input unbounded when a producer on the same thread would push into
//! it without its consumer getting to run first: a [crate::Pushes] side path into a node that the
//! same owner schedules, an upstream shared by two owners, a back-edge, or a [Writer] on the
//! reader's thread. Such a push blocks forever, logged as "blocked" with no "unblocked".
//!
//! A "blocked" with no "unblocked" can also be a producer starved by others refilling the edge
//! first; the logs can't tell the two apart.
//!
//! Scheduling can't cycle: [crate::Via] and [crate::Schedules] move an upstream into its owner.
//! Data can, through a [crate::Pushes] back-edge into an upstream input; that isn't detected, so keep such an input
//! unbounded (above). Between two threads, bound edges in one direction only: bounded both ways,
//! each can block on the other.

pub(crate) mod multiedge;
mod pulled;
mod schedule;
mod sink;
mod stream;
mod via;
mod work_each;
mod workable;

pub use pulled::Pulled;
pub use schedule::Schedules;
pub use sink::{DynSink, Sink};
pub use stream::{Streams, ThreadStream, ThreadStreamHandle};
pub use via::{OwnedWork, Sided, Upstream, Whole};
pub(crate) use work_each::work_each;
pub use workable::Workable;

pub use crate::node::bifurcation::work::node::Bifurcation;
pub use crate::node::biunion::work::node::Biunion;
pub use crate::node::line::work::node::Line;
pub use crate::reader::work::{Reader, Reads, Tees, tee};
pub use crate::writer::push::{Expects, Writer, Writes};
