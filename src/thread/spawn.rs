//! [Spawn]: a thread on its way to its own OS thread, with the options for it.

mod never_started;

pub use never_started::NeverStarted;

use super::join::Spawned;
use crate::error::Error;
use std::thread::{Builder, Scope};

/// A thread that will get its own OS thread, and the options for it. It can only be started or
/// added to a [ThreadBundle](crate::ThreadBundle), never run on the current thread, so nothing
/// set here can be silently ignored.
///
/// ```ignore
/// let encoder = encoder.with().stack_size(32 * 1024);
/// bundle.add(decoder).add(encoder);
/// ```
///
/// Configured means spawned: a poll thread can [run](crate::poll::Thread::run) on the current
/// thread, but once configured there is no `run` to ignore the options.
///
/// ```compile_fail,E0599
/// use areamy::{DefaultThread, poll};
/// let configured = poll::Thread::<DefaultThread>::new().with().stack_size(64 * 1024);
/// configured.run();
/// ```
///
/// A spawn the OS refuses still ends the thread like any exit: its `on_done` callbacks fire with a
/// [NeverStarted] `Done::Error`, and `join` returns it with the OS's reason as `Join::Error`.
#[must_use = "a Spawn does nothing until started or added to a bundle"]
pub struct Spawn<ThreadType> {
    pub(crate) thread: ThreadType,
    pub(crate) options: Options,
}

/// How an OS thread is spawned. Set through [Spawn]; unnameable outside the crate.
#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// Bytes; the platform default when `None`.
    stack_size: Option<usize>,
}

impl<ThreadType> Spawn<ThreadType> {
    pub(crate) fn new(thread: ThreadType) -> Self {
        Self {
            thread,
            options: Options::default(),
        }
    }

    /// Stack size of the OS thread, in bytes. Otherwise the platform default, which on ESP-IDF
    /// (the menuconfig pthread default, a few KB) is often too small for deep poll frames.
    pub fn stack_size(mut self, bytes: usize) -> Self {
        self.options.stack_size = Some(bytes);
        self
    }
}

impl Options {
    /// Spawn `task` into `scope`, on an OS thread named `thread_name`.
    ///
    /// The name comes as a string, not a `ThreadId` type parameter: a bundle holds its threads
    /// type-erased, so only [ThreadId::name](crate::ThreadId::name) survives to here.
    pub(crate) fn spawn<'scope, 'env, TaskType>(
        self,
        scope: &'scope Scope<'scope, 'env>,
        thread_name: &'static str,
        task: TaskType,
    ) -> Spawned<'scope>
    where
        TaskType: FnOnce() -> Result<(), Error> + Send + 'scope,
    {
        let mut builder = Builder::new().name(thread_name.into());
        // Unset keeps the platform default, as `scope.spawn` would.
        if let Some(bytes) = self.stack_size {
            builder = builder.stack_size(bytes);
        }
        // A `Builder` reports a refused spawn instead of panicking like `scope.spawn`; the
        // handle keeps it for `join` to return.
        let thread = builder.spawn_scoped(scope, task).map_err(|reason| {
            NeverStarted {
                thread: thread_name,
                reason: Some(reason),
            }
            .into_error()
        });
        Spawned::new(thread, thread_name)
    }
}

#[cfg(test)]
mod tests;
