//! Work graphs: blocking nodes scheduled by their children through [Workable].
//!
//! A work thread follows one call stack, so it waits on one thing at a time: while a node
//! blocks (say a parentless [Line] waiting on its [Writer]), its siblings under the same owner
//! don't run, even if they have data. Merge independent sources with [crate::Push] side paths
//! into one input, a [Biunion] (its two inputs wait together), or the poll paradigm.
//!
//! A [Biunion] takes left input before right, so a left side that never empties starves the
//! right. Fair scheduling between sources is what the poll paradigm is for.
//!
//! A diamond whose side branch has no parent of its own (P feeds C by [Bidi] and D by
//! [crate::Push]; D feeds C by [Bidi]) isn't supported: D blocks waiting for P, which only runs
//! when C schedules it, on the same stack.
//!
//! # Bounded inputs
//!
//! Bound an input only when its producers are the node's own [Bidi] parents or run on other
//! threads. A parent refused by such an input yields and finds room when rescheduled, because
//! the node drains its input before scheduling parents again. Keep an input unbounded when a producer on the same thread would push into
//! it without its consumer getting to run first: a [crate::Push] side path into a node that the
//! same owner schedules, a parent shared by two owners, a back-edge, or a [Writer] on the
//! reader's thread. Such a push blocks forever, logged as "blocked" with no "unblocked".
//!
//! A "blocked" with no "unblocked" can also be a producer starved by others refilling the edge
//! first; the logs can't tell the two apart.

mod bidi;
pub(crate) mod multiedge;
mod pulled;
mod schedule;
mod stream;
mod work_each;
mod workable;

pub use bidi::Bidi;
pub use pulled::Pulled;
pub use schedule::Schedule;
pub use stream::{ThreadStream, ThreadStreamHandle};
pub(crate) use work_each::work_each;
pub use workable::Workable;

pub use crate::node::bifurcation::work::node::Bifurcation;
pub use crate::node::biunion::work::node::Biunion;
pub use crate::node::line::work::node::Line;
pub use crate::reader::work::{Reader, tee};
pub use crate::writer::push::Writer;
