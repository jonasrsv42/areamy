//! [`ThreadBundle`] and [`ThreadBundleHandle`] — the user-facing
//! bundle API.

use super::first_error::{self, OnFirstError};
use crate::thread::done::Failure;
use crate::thread::join::{BundleJoin, Spawned};
use crate::thread::type_erase::TypeErasedInternalThreadStream;
use std::thread::Scope;

/// A bundle of idle threads that can be started together.
#[derive(Default)]
pub struct ThreadBundle<'params> {
    threads: Vec<Box<dyn TypeErasedInternalThreadStream<'params> + 'params>>,
    on_first_error: Vec<OnFirstError>,
}

/// A handle to running threads, returned by [`ThreadBundle::start`].
pub struct ThreadBundleHandle<'threads> {
    entries: Vec<Spawned<'threads>>,
}

impl<'params> ThreadBundle<'params> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a [ThreadStream](crate::work::ThreadStream) or [poll::Thread](crate::poll::Thread), or
    /// one configured with `with()` (a [Spawn](crate::thread::Spawn)).
    pub fn add(&mut self, thread: impl TypeErasedInternalThreadStream<'params>) -> &mut Self {
        self.threads.push(Box::new(thread));
        self
    }

    /// Register a callback to fire when the first thread exits via
    /// [`Done::Error`](crate::thread::Done::Error) or
    /// [`Done::Panic`](crate::thread::Done::Panic).
    ///
    /// Each registered callback fires **at most once per bundle
    /// lifetime**, all of them in registration order, on whichever
    /// thread wins the race. None fire if every thread exits via
    /// [`Done::Close`](crate::thread::Done::Close). A thread the OS
    /// refused to spawn counts as an error, a
    /// [`NeverStarted`](crate::thread::NeverStarted) fired from `start`;
    /// its OS reason comes from `join`.
    ///
    /// **Race resolution:** if multiple threads error nearly
    /// simultaneously, exactly one wins the mutex/take and drains
    /// the callback list. Which thread "wins" is non-deterministic.
    /// Callbacks run on the winning thread, same constraints as
    /// [`ThreadStream::on_done`](crate::work::ThreadStream::on_done)
    /// (no block, no panic). Panics are caught and logged so one
    /// misbehaving callback cannot abort the process or prevent its
    /// siblings from firing.
    ///
    /// **Not provided** (and the caller must handle):
    /// - **Cross-bundle deduplication.** Each `ThreadBundle` has its
    ///   own latch. A new bundle gets a fresh one. If your event-loop
    ///   channel outlives multiple bundle generations, tag events
    ///   with a generation ID and discard stale ones on dequeue.
    /// - **Ordering vs `bundle.join()`.** Callbacks may fire before,
    ///   during, or after `bundle.join()` — independent paths.
    pub fn on_first_error<F>(&mut self, callback: F) -> &mut Self
    where
        F: FnOnce(&Failure) + Send + 'static,
    {
        self.on_first_error.push(Box::new(callback));
        self
    }

    /// Spawn all bundled threads into the provided `scope`. Returns a
    /// handle bound to `'threads` (the scope's lifetime).
    pub fn start<'threads>(
        mut self,
        scope: &'threads Scope<'threads, 'params>,
    ) -> ThreadBundleHandle<'threads>
    where
        'params: 'threads,
    {
        first_error::inject(&mut self.threads, core::mem::take(&mut self.on_first_error));

        let entries = self
            .threads
            .into_iter()
            .map(|t| {
                let (thread_name, options) = (t.thread_name(), t.spawn_options());
                options.spawn(scope, thread_name, t.into_task())
            })
            .collect();

        ThreadBundleHandle { entries }
    }
}

impl<'threads> ThreadBundleHandle<'threads> {
    /// Join all threads. Returns one [`Join`](crate::thread::Join)
    /// per registered thread, in registration order, wrapped in
    /// [`BundleJoin`].
    pub fn join(self) -> BundleJoin {
        BundleJoin::new(self.entries.into_iter().map(Spawned::join).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::ThreadBundle;
    use crate::tests::support::{DEADLINE, deadline};
    use crate::thread::Join;
    use crate::thread::fixtures::{ImmediateClose, Panicker, ThreadA, ThreadB, WorkError};
    use crate::work::{Streams, ThreadStream};

    #[test]
    fn start_and_join_empty() {
        let bundle = ThreadBundle::new();
        std::thread::scope(|s| {
            let handle = bundle.start(s);
            assert!(handle.join().is_empty());
        });
    }

    #[test]
    fn heterogeneous_threads() {
        let _deadline = deadline(DEADLINE);
        let mut bundle = ThreadBundle::new();
        bundle
            .add(ThreadStream::<'_, ThreadA>::new())
            .add(ThreadStream::<'_, ThreadB>::new());
        std::thread::scope(|s| {
            let handle = bundle.start(s);
            let results = handle.join();
            assert_eq!(results.len(), 2);
            assert!(matches!(results[0], Join::Ok));
            assert!(matches!(results[1], Join::Ok));
        });
    }

    #[test]
    fn chaining() {
        let mut bundle = ThreadBundle::new();
        bundle
            .add(ThreadStream::<'_, ThreadA>::new())
            .add(ThreadStream::<'_, ThreadA>::new())
            .add(ThreadStream::<'_, ThreadB>::new());
        assert_eq!(bundle.threads.len(), 3);
    }

    #[test]
    fn work_error_appears_in_results() {
        let thread_a = WorkError::<ThreadA>::new().stream();

        let thread_b = ImmediateClose::<ThreadB>::new().stream();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        std::thread::scope(|s| {
            let results = bundle.start(s).join();
            assert!(matches!(results[0], Join::Error(_)));
            assert!(matches!(results[1], Join::Ok));
        });
    }

    #[test]
    fn panic_appears_as_panic_variant() {
        let thread_a = Panicker::<ThreadA>::new().stream();

        let thread_b = ThreadStream::<'_, ThreadB>::new();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        std::thread::scope(|s| {
            let results = bundle.start(s).join();
            assert!(matches!(results[0], Join::Panic(_)));
            assert!(matches!(results[1], Join::Ok));
        });
    }

    #[test]
    fn multiple_panics_each_recorded() {
        let thread_a = Panicker::<ThreadA>::new().stream();

        let thread_b = Panicker::<ThreadB>::new().stream();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        std::thread::scope(|s| {
            let results = bundle.start(s).join();
            assert!(matches!(results[0], Join::Panic(_)));
            assert!(matches!(results[1], Join::Panic(_)));
        });
    }
}
