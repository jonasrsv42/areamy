//! Internal type-erasure for [`ThreadBundle`].
//!
//! Lets the bundle store a mixed bag of [`ThreadStream`] (sync) and
//! [`poll::Thread`](crate::thread::poll::stream::Thread) (async)
//! behind a uniform `Box<dyn ...>` interface.
//!
//! [`ThreadBundle`]: crate::thread::ThreadBundle
//! [`ThreadStream`]: crate::thread::ThreadStream

use super::callback::OnDone;
use super::join::Join;
use super::poll::stream as poll;
use super::{ThreadId, ThreadStream, ThreadStreamHandle};

/// Trait for type-erased idle threads that can be started and joined.
pub trait TypeErasedInternalThreadStream: Send {
    /// Register a callback to fire when this thread exits. See
    /// [`ThreadStream::on_done`] for the contract.
    fn on_done(&mut self, callback: OnDone);

    fn start(self: Box<Self>) -> Box<dyn TypeErasedInternalThreadStreamHandle>;
}

/// Trait for type-erased running thread handles.
pub trait TypeErasedInternalThreadStreamHandle: Send {
    fn join(self: Box<Self>) -> Join;
}

impl<ThreadIdType: ThreadId + 'static> TypeErasedInternalThreadStream
    for ThreadStream<ThreadIdType>
{
    fn on_done(&mut self, callback: OnDone) {
        ThreadStream::on_done(self, move |done| callback(done));
    }

    fn start(self: Box<Self>) -> Box<dyn TypeErasedInternalThreadStreamHandle> {
        Box::new((*self).start())
    }
}

impl TypeErasedInternalThreadStreamHandle for ThreadStreamHandle {
    fn join(self: Box<Self>) -> Join {
        (*self).join()
    }
}

impl<ThreadIdType: ThreadId + 'static> TypeErasedInternalThreadStream
    for poll::Thread<ThreadIdType>
{
    fn on_done(&mut self, callback: OnDone) {
        poll::Thread::on_done(self, move |done| callback(done));
    }

    fn start(self: Box<Self>) -> Box<dyn TypeErasedInternalThreadStreamHandle> {
        Box::new((*self).start())
    }
}

impl TypeErasedInternalThreadStreamHandle for poll::ThreadHandle {
    fn join(self: Box<Self>) -> Join {
        (*self).join()
    }
}
