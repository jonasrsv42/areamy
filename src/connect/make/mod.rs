//!
//! Utilities for creating [crate::connect::marker::Connection] between Nodes.
//!
//!
mod core;
pub mod work;

pub use core::{make_bidi, make_push, make_work};
