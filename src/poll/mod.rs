//! Async polling primitives — edge markers, combinators, future routine.

pub mod future;
pub mod join;
pub mod select;

pub use crate::connect::poll::edge::{Async, AsyncIn, Deferred, Edge, Linktime, Null, Sync};
pub use crate::node::line::poll::factory::PollLineRoutineFactory;
pub use future::{FutureRoutine, Input, InputConsumer, OutputProducer};
pub use join::Join;
pub use select::Select;
