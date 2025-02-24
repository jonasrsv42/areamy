//! Collection of [crate::graph] nodes.
//!
//! A [crate::node] is usually a wrapper around a [routine]. A [crate::node] does not
//! need to run a routine and could contain computation directly but we commonly have [crate::node]
//! run a [routine], such as [crate::LineRoutine], as the scheduling part of a node is usually identical across
//! multiple routine implementations.
//!
//! As an example look at the [crate::Workable::work] implementation in
//! [crate::node::line::work::node::Line], this implementation is shared across many [crate::LineRoutine] implementations.

pub mod bifurcation;
pub mod biunion;
pub mod line;
pub mod routine;

pub use bifurcation::{BifurcationReader, BifurcationRoutine};
pub use biunion::{BiunionReader, BiunionRoutine};
pub use line::{LineReader, LineRoutine};
pub use routine::{Flush, Next, Send, Name};

