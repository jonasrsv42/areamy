//! Internal type-erasure for [`ThreadBundle`].
//!
//! Lets the bundle store a mixed bag of [`ThreadStream`] (sync) and
//! [`poll::Thread`](crate::thread::poll::stream::Thread) (async)
//! behind a uniform `Box<dyn ...>` interface.
//!
//! Each erased thread exposes a `run()` that drives its work loop
//! synchronously. The bundle's [`start`](crate::thread::ThreadBundle::start)
//! spawns each `run()` into the user-provided scope.
//!
//! [`ThreadBundle`]: crate::thread::ThreadBundle
//! [`ThreadStream`]: crate::thread::ThreadStream

use super::callback::OnDone;
use super::poll::stream as poll;
use super::{ThreadId, ThreadStream};
use crate::error::Error;

/// Trait for type-erased idle threads. Bundle owns these and calls
/// [`run`](Self::run) inside `scope.spawn(...)` at start time.
pub trait TypeErasedInternalThreadStream<'params>: Send + 'params {
    /// Register a callback to fire when this thread exits. See
    /// [`ThreadStream::on_done`] for the contract.
    fn on_done(&mut self, callback: OnDone);

    /// Run the thread's work loop. Called inside `scope.spawn(...)`.
    /// Returns `Ok(())` on clean exit, `Err` on failure.
    fn run(self: Box<Self>) -> Result<(), Error>;

    /// `type_name::<ThreadIdType>()` for panic diagnostics.
    fn thread_name(&self) -> &'static str;
}

impl<'params, ThreadIdType: ThreadId> TypeErasedInternalThreadStream<'params>
    for ThreadStream<'params, ThreadIdType>
{
    fn on_done(&mut self, callback: OnDone) {
        ThreadStream::on_done(self, move |done| callback(done));
    }

    fn run(self: Box<Self>) -> Result<(), Error> {
        (*self).run()
    }

    fn thread_name(&self) -> &'static str {
        std::any::type_name::<ThreadIdType>()
    }
}

impl<'params, ThreadIdType: ThreadId> TypeErasedInternalThreadStream<'params>
    for poll::Thread<'params, ThreadIdType>
{
    fn on_done(&mut self, callback: OnDone) {
        poll::Thread::on_done(self, move |done| callback(done));
    }

    fn run(self: Box<Self>) -> Result<(), Error> {
        (*self).run()
    }

    fn thread_name(&self) -> &'static str {
        std::any::type_name::<ThreadIdType>()
    }
}
