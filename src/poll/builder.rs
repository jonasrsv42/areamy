//! [Builder] sets a poll node's options before it exists, from [Thread::builder]: its inputs
//! mint the senders every producer holds, so a bound can't change after.

use crate::poll::thread::Thread;
use crate::thread::ThreadId;

/// Picks the kind of poll node to configure: `.line()` or `.biunion()`.
pub struct Builder<'thread, 'params, ThreadIdType: ThreadId> {
    pub(crate) thread: &'thread mut Thread<'params, ThreadIdType>,
}

impl<'params, ThreadIdType: ThreadId> Thread<'params, ThreadIdType> {
    /// Options for a node on this thread, set before it exists.
    pub fn builder(&mut self) -> Builder<'_, 'params, ThreadIdType> {
        Builder { thread: self }
    }
}
