//! Work graphs: blocking nodes scheduled by their children through [Workable].

mod connect;
mod make;
pub(crate) mod multiedge;
mod workable;

pub use connect::Connect;
pub use make::{make_bidi, make_work};
pub use workable::Workable;

pub use crate::node::bifurcation::work::builder::make_bifurcation;
pub use crate::node::bifurcation::work::node::Bifurcation;
pub use crate::node::biunion::work::builder::make_biunion;
pub use crate::node::biunion::work::node::Biunion;
pub use crate::node::line::work::bridge::from_pull;
pub use crate::node::line::work::node::{Line, make_line};
pub use crate::reader::work::{Reader, tee};
pub use crate::writer::push::Writer;
