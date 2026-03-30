//! Async polling primitives — edge markers, combinators.

pub mod join;
pub mod select;

pub use crate::connect::poll::marker::{Async, AsyncIn, Deferred, EdgeKind, Linktime, Null, Sync};
pub use join::Join;
pub use select::Select;
