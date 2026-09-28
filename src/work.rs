//! Work graphs: blocking nodes scheduled by their children through [Workable].
//!
//! A work thread follows one call stack, so it waits on one thing at a time: while a node
//! blocks (say a parentless [Line] waiting on its [Writer]), its siblings under the same owner
//! don't run, even if they have data. Merge independent sources with [crate::Push] side paths
//! into one input, a [Biunion] (its two inputs wait together), or the poll paradigm.

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
