//! Flush/close contract, see [crate::Message::Flush]. A failure here is a broken promise, not a
//! changed detail.
//!
//! - **Flush, then close, delivers.** A Flush pushed before close is fully processed (held
//!   output, then Flush, then close) by every edge and node type.
//! - **A producer's close ends that producer only.** An edge closes once every producer into it
//!   is done; closing the receiver closes it for all.
//! - **A merge ends with its last input**, so every source's flushed data arrives, whatever order
//!   they close in, even when one closes before another's Flush has travelled ([merge]).
//! - **Close is fast.** Data not followed by a Flush may be dropped.

mod bifurcation;
mod biunion;
mod edge;
mod graph;
mod line;
mod merge;
mod mock;
