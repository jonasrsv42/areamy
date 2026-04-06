//! Async polling primitives — edge markers, combinators, future routine.

pub mod future;
pub mod join;
pub mod select;

pub use crate::connect::poll::edge::{
    Async, AsyncIn, Deferred, Edge, Linktime, Null, PollEdge, Sync,
};
pub use crate::connect::poll::graph::{Graph, GraphBuilder, GraphNode};
pub use crate::node::line::poll::factory::LineRoutineFactory;
pub use crate::node::line::poll::routine::LineRoutine;
pub use crate::thread::poll::stream::{Thread, ThreadHandle};
pub use future::{FutureRoutine, Input, InputConsumer, OutputProducer};
pub use join::Join;
pub use select::Select;
