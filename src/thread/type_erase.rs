//! Internal type-erasure for [`ThreadBundle`].
//!
//! Lets the bundle store a mixed bag of [`ThreadStream`] (sync) and
//! [`poll::Thread`] (async)
//! behind a uniform `Box<dyn ...>` interface.
//!
//! Each erased thread exposes a `run()` that drives its work loop
//! synchronously. The bundle's [`start`](crate::thread::ThreadBundle::start)
//! spawns each `run()` into the user-provided scope.
//!
//! [`ThreadBundle`]: crate::thread::ThreadBundle
//! [`ThreadStream`]: crate::work::ThreadStream

use super::callback::OnDone;
use super::{Spawn, SpawnOptions, ThreadId};
use crate::error::Error;
use crate::poll::thread as poll;
use crate::work::ThreadStream;

/// The bundle's task, run on its own OS thread.
pub type Task<'params> = Box<dyn FnOnce() -> Result<(), Error> + Send + 'params>;

/// Trait for type-erased idle threads. Bundle owns these and spawns each one's
/// [`into_task`](Self::into_task) at start time.
pub trait TypeErasedInternalThreadStream<'params>: Send + 'params {
    /// Register a callback to fire when this thread exits. See
    /// [`ThreadStream::on_done`] for the contract.
    fn on_done(&mut self, callback: OnDone);

    /// The thread's work loop as a task: `Ok(())` on clean exit, `Err` on failure. Its exit
    /// guard already holds the `on_done` callbacks, so a task dropped unrun still fires them.
    fn into_task(self: Box<Self>) -> Task<'params>;

    /// [ThreadId::name], carried past the erasure: names the OS thread and labels a panic.
    fn thread_name(&self) -> &'static str;

    /// How the bundle spawns its OS thread: set through a [Spawn], the defaults otherwise.
    fn spawn_options(&self) -> SpawnOptions;
}

/// A configured thread: the bundle runs the thread inside with its options.
impl<'params, ThreadType> TypeErasedInternalThreadStream<'params> for Spawn<ThreadType>
where
    ThreadType: TypeErasedInternalThreadStream<'params>,
{
    fn on_done(&mut self, callback: OnDone) {
        self.thread.on_done(callback);
    }

    fn into_task(self: Box<Self>) -> Task<'params> {
        Box::new(self.thread).into_task()
    }

    fn thread_name(&self) -> &'static str {
        self.thread.thread_name()
    }

    fn spawn_options(&self) -> SpawnOptions {
        self.options
    }
}

impl<'params, ThreadIdType: ThreadId> TypeErasedInternalThreadStream<'params>
    for ThreadStream<'params, ThreadIdType>
{
    fn on_done(&mut self, callback: OnDone) {
        ThreadStream::on_done(self, move |done| callback(done));
    }

    fn into_task(self: Box<Self>) -> Task<'params> {
        Box::new((*self).into_task())
    }

    fn thread_name(&self) -> &'static str {
        ThreadIdType::name()
    }

    fn spawn_options(&self) -> SpawnOptions {
        SpawnOptions::default()
    }
}

impl<'params, ThreadIdType: ThreadId> TypeErasedInternalThreadStream<'params>
    for poll::Thread<'params, ThreadIdType>
{
    fn on_done(&mut self, callback: OnDone) {
        poll::Thread::on_done(self, move |done| callback(done));
    }

    fn into_task(self: Box<Self>) -> Task<'params> {
        Box::new((*self).into_task())
    }

    fn thread_name(&self) -> &'static str {
        ThreadIdType::name()
    }

    fn spawn_options(&self) -> SpawnOptions {
        SpawnOptions::default()
    }
}
