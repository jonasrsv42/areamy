//! Work graphs: blocking nodes scheduled by their children through [Workable].

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
