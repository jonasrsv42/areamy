//! Edge kind markers for async node builders.
//!
//! [`Sync`] and [`Async`] describe the edge type (cross-thread vs local).
//! [`Deferred`] resolves to either based on how the node is wired.
//!
//! Used as type parameters on [`Node`](crate::node::line::poll::builder::node::Node)
//! to select storage and control which wiring methods are available.

use super::null::Null;
use crate::edge::fanout::Fanout;
use crate::graph::marker::Connection;
use crate::poll::graph::Reserved;
use crate::poll::input;
use crate::poll::{self, Sink};
use crate::signal::Origin;
use crate::thread::ThreadId;

/// Trait for edge markers. Uses GATs to resolve storage types.
pub trait Edge: Connection {
    type Input<'params, InType, SignalType: Origin, ThreadIdType: ThreadId>;
    type Output<'params, OutType, SignalType: Origin>;
    /// The input's node, reserved at mint: `Deferred` and `Async` hold it; `Sync` keeps it in its
    /// input.
    type Reservation<ThreadIdType: ThreadId>;
}

/// Sync edge — cross-thread, uses [`input::sync::Receiver`]
/// (a [`waking`](crate::edge::waking) edge).
pub struct Sync;

/// Async edge — same-thread, uses [`poll`](super::poll) edges (no Mutex).
pub struct Async;

/// Zero-sized marker indicating that output is provided at build time
/// by a downstream node, not stored in the builder.
#[derive(Default)]
pub struct Linktime;

/// Deferred edge — resolves to [`Sync`] or [`Async`] based on wiring.
pub struct Deferred;

/// Direct output edge — a caller-provided [`Sink`] stored in the builder.
pub struct Direct;

impl Connection for Sync {}
impl Connection for Async {}
impl Connection for Deferred {}
impl Connection for Direct {}

impl Edge for Sync {
    type Input<'params, InType, SignalType: Origin, ThreadIdType: ThreadId> =
        input::sync::Input<InType, SignalType, ThreadIdType>;
    type Output<'params, OutType, SignalType: Origin> =
        Fanout<poll::DynSink<'params, OutType, SignalType>>;
    type Reservation<ThreadIdType: ThreadId> = ();
}

impl Edge for Async {
    type Input<'params, InType, SignalType: Origin, ThreadIdType: ThreadId> =
        input::r#async::Input<'params, InType, SignalType, ThreadIdType>;
    type Output<'params, OutType, SignalType: Origin> = Linktime;
    type Reservation<ThreadIdType: ThreadId> = Reserved<ThreadIdType>;
}

impl Edge for Deferred {
    type Input<'params, InType, SignalType: Origin, ThreadIdType: ThreadId> =
        Null<InType, SignalType>;
    type Output<'params, OutType, SignalType: Origin> = Null<OutType, SignalType>;
    type Reservation<ThreadIdType: ThreadId> = Reserved<ThreadIdType>;
}

impl Edge for Direct {
    type Input<'params, InType, SignalType: Origin, ThreadIdType: ThreadId> =
        Null<InType, SignalType>;
    type Output<'params, OutType, SignalType: Origin> =
        Box<dyn Sink<DataType = OutType, SignalType = SignalType> + Send + 'params>;
    type Reservation<ThreadIdType: ThreadId> = ();
}
