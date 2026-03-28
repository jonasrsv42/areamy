//! [Spawnable] trait for builders that can produce [Pollable] nodes.
//!
//! The [super::stream::AsyncThread] stores `Box<dyn Spawnable>` to
//! accept any builder type regardless of its [crate::marker::Linkage] role.

use crate::{Pollable, ThreadId};

/// A builder that can be spawned into running [Pollable] nodes.
///
/// Implemented by each builder type (TerminalBuilder, etc.) to produce
/// nodes for the async thread.
pub trait Spawnable<ThreadIdType: ThreadId>: Send {
    fn spawn(self: Box<Self>) -> Vec<Box<dyn Pollable<ThreadId = ThreadIdType>>>;
}
