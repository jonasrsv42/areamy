//! Closable wrapper around [`crate::connect::poll::runtime::Runtime`].
//!
//! The base `Runtime` is a flat `Vec<Node>`. The poll loop wants to
//! drop a node *in place* as soon as it returns `Ready` or `Closed`
//! so its close-on-drop cascade fires immediately — but it must keep
//! indexing stable because node IDs in the ready queue refer to slot
//! positions. `ClosableRuntime` wraps each slot in `Option<Node>`:
//! a slot becomes `None` when its node terminates, indexing stays
//! valid, and a wake for a `None` slot is observable.

use crate::ThreadId;
use crate::connect::poll::runtime::{Node, Runtime};

pub struct ClosableRuntime<ThreadIdType: ThreadId> {
    pub nodes: Vec<Option<Node<ThreadIdType>>>,
}

impl<ThreadIdType: ThreadId> From<Runtime<ThreadIdType>> for ClosableRuntime<ThreadIdType> {
    fn from(runtime: Runtime<ThreadIdType>) -> Self {
        Self {
            nodes: runtime.nodes.into_iter().map(Some).collect(),
        }
    }
}
