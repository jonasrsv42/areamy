//! Poll graphs: event-driven nodes driven by wakers through [Pollable].

mod budget;
pub mod context;
pub mod edge;
pub mod future;
pub mod graph;
pub mod input;
pub(crate) mod limit;
pub mod marker;
mod phase;
mod pollable;
pub mod queue;
pub mod race;
pub mod runtime;
pub(crate) mod sink;
pub mod sleep;
pub mod thread;
pub mod timer;
pub mod traits;
pub mod try_join;
pub mod wakers;

pub use budget::BUDGET;
pub use context::Context;
pub use edge::{Async, Deferred, Direct, Edge, Linktime, Null, Sync};
pub use graph::{Graph, GraphBuilder, GraphNode};
pub use marker::NodeId;
pub use phase::Phase;
pub use pollable::Pollable;
pub use race::{Either, race};
pub use sink::{DynSink, Sink};
pub use sleep::{SleepFut, sleep, sleep_until};
pub use thread::{Thread, ThreadHandle};
pub use timer::{Timer, TimerKey};
pub use traits::AsyncParent;
pub use try_join::try_join;
pub use wakers::{ThreadLocalWakerAllocator, WakerAllocator};

pub use crate::node::biunion::poll::factory::{
    BiunionInputs, BiunionRoutineFactory, BiunionWakers,
};
pub use crate::node::line::poll::factory::{LineRoutineFactory, LineWakers};
pub use crate::node::line::poll::routine::LineRoutine;
