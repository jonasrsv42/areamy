//! [Runtime] — the poll loop's nodes, each at its id with the context it's polled with; a finished
//! one is `None`.

use crate::poll::{Context, Pollable};
use crate::thread::ThreadId;

use alloc::vec::Vec;

pub(crate) struct Node<'params, ThreadIdType: ThreadId> {
    pub(crate) pollable: Box<dyn Pollable<ThreadId = ThreadIdType> + 'params>,
    pub(crate) cx: Context,
}

/// Each node at its id.
pub(crate) struct Runtime<'params, ThreadIdType: ThreadId> {
    pub(crate) nodes: Vec<Option<Node<'params, ThreadIdType>>>,
}
