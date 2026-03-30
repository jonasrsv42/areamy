//! Async polling primitives — edge markers, combinators, future routine.

pub mod future;
pub mod join;
pub mod select;

pub use crate::connect::poll::marker::{Async, AsyncIn, Deferred, EdgeKind, Linktime, Null, Sync};
pub use future::{FutureRoutine, Input, Queue};
pub use join::Join;
pub use select::Select;
