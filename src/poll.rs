//! Poll graphs: event-driven nodes driven by wakers through [Pollable].
//!
//! # Wake order
//!
//! A poll thread polls its woken nodes in batches, newest wake first: a batch is every node woken
//! since the previous batch began, and wakes before a node's poll coalesce into it. A wake after
//! its node's poll, `local` or `sync` alike, joins the next batch. Expired timers join each batch
//! as it begins, so they lead it.
//!
//! A wake polls only its own node, at any graph size; a poll still means "may have work": a wake
//! that lands while its node is being polled polls it once more.

mod budget;
mod builder;
pub mod context;
pub mod edge;
pub mod future;
pub mod graph;
pub mod input;
pub(crate) mod limit;
pub mod marker;
mod phase;
mod pollable;
pub mod race;
pub mod runtime;
pub(crate) mod scheduler;
pub(crate) mod sink;
pub mod sleep;
pub mod thread;
pub mod timer;
pub mod traits;
pub mod try_join;
pub mod wakers;

pub use budget::{BUDGET, Budget};
pub use builder::Builder;
pub use context::Context;
pub use edge::{Async, Deferred, Direct, Edge, Linktime, Null, Sync};
pub use future::line::FutureRoutine;
pub use graph::{Graph, GraphBuilder, GraphNode};
pub use marker::NodeId;
pub use phase::Phase;
pub use pollable::Pollable;
pub use race::{Either, race};
pub use sink::{DynSink, Sink};
pub use sleep::{SleepFut, sleep, sleep_until};
pub use thread::{Thread, ThreadHandle};
pub use timer::{Timer, TimerKey};
pub use traits::{AsyncParent, OwnedPoll};
pub use try_join::try_join;
pub use wakers::{ThreadLocalWakerAllocator, WakerAllocator};

pub use crate::node::biunion::poll::factory::{
    BiunionInputs, BiunionRoutineFactory, BiunionWakers,
};
pub use crate::node::biunion::poll::routine::BiunionRoutine;
pub use crate::node::line::poll::factory::{LineRoutineFactory, LineWakers};
pub use crate::node::line::poll::routine::LineRoutine;
