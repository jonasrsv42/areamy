//! Flush/close ordering contract, see [crate::Message::Flush].
//!
//! Guaranteed on a single edge: a Flush pushed before close is fully
//! processed — held output, then Flush, then close — by every edge and
//! node type. Not guaranteed: close without Flush keeps nothing, and a
//! merge node closing on one input drops what its other inputs hold.

mod bifurcation;
mod biunion;
mod edge;
mod graph;
mod line;
mod mock;
