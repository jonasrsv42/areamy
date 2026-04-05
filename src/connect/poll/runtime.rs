//! Async poll runtime.
//!
//! [Runtime] holds pollable nodes paired with their wakers for the poll loop.
//! Built via [ThreadLocalWakerAllocator::build](crate::connect::poll::wakers::ThreadLocalWakerAllocator::build).

use crate::connect::waker;
use crate::{Pollable, ThreadId};

use alloc::vec::Vec;

/// A pollable node paired with its waker for the poll loop.
pub struct Node<ThreadIdType: ThreadId> {
    pub pollable: Box<dyn Pollable<ThreadId = ThreadIdType>>,
    pub waker: waker::Waker,
}

/// Finalized runtime. All slots filled, ready for the poll loop.
pub struct Runtime<ThreadIdType: ThreadId> {
    pub nodes: Vec<Node<ThreadIdType>>,
}
