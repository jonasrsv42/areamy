//! [Spawnable] trait for builders that can produce [Pollable] nodes.
//!
//! The [super::stream::AsyncThread] stores `Box<dyn Spawnable>` to
//! accept any builder type regardless of its [crate::marker::Linkage] role.
//!
//! Each node is returned with its [NodeId] so the async thread can place
//! it at the correct index, matching its waker.

use crate::{Pollable, ThreadId};

/// A node ID assigned by [super::stream::AsyncThread] at builder creation.
/// Matches the waker's node_id so the poll loop wakes the correct node.
pub type NodeId = usize;

/// A builder that can be spawned into running [Pollable] nodes.
///
/// Returns `(NodeId, Node)` pairs so the async thread can place each node
/// at the index matching its waker.
pub trait Spawnable<ThreadIdType: ThreadId>: Send {
    fn spawn(self: Box<Self>) -> Vec<(NodeId, Box<dyn Pollable<ThreadId = ThreadIdType>>)>;
}
