//! Async poll runtime.
//!
//! [Runtime] holds pollable nodes paired with their contexts for the poll loop.
//! Built via [ThreadLocalWakerAllocator::build](crate::poll::wakers::ThreadLocalWakerAllocator::build).

use crate::poll::{Context, Pollable};
use crate::thread::ThreadId;

use alloc::vec::Vec;

/// A pollable node paired with its context for the poll loop.
pub struct Node<'params, ThreadIdType: ThreadId> {
    pub pollable: Box<dyn Pollable<ThreadId = ThreadIdType> + 'params>,
    pub cx: Context,
}

/// Finalized runtime. All slots filled, ready for the poll loop.
pub struct Runtime<'params, ThreadIdType: ThreadId> {
    pub nodes: Vec<Node<'params, ThreadIdType>>,
}
