//! [`ThreadBundle`] and [`ThreadBundleHandle`] — the user-facing
//! bundle API.

use super::first_error::{self, OnFirstError};
use crate::error::Error;
use crate::fatal;
use crate::thread::done::Failure;
use crate::thread::join::{BundleJoin, Join};
use crate::thread::type_erase::TypeErasedInternalThreadStream;
use std::thread::{Scope, ScopedJoinHandle};

/// A bundle of idle threads that can be started together.
pub struct ThreadBundle<'params> {
    threads: Vec<Box<dyn TypeErasedInternalThreadStream<'params> + 'params>>,
    on_first_error: Vec<OnFirstError>,
}

impl<'params> Default for ThreadBundle<'params> {
    fn default() -> Self {
        Self {
            threads: Vec::new(),
            on_first_error: Vec::new(),
        }
    }
}

/// A scoped join handle paired with its thread name (for panic diagnostics).
struct BundleEntry<'threads> {
    handle: ScopedJoinHandle<'threads, Result<(), Error>>,
    thread_name: &'static str,
}

/// A handle to running threads, returned by [`ThreadBundle::start`].
pub struct ThreadBundleHandle<'threads> {
    entries: Vec<BundleEntry<'threads>>,
}

impl<'params> ThreadBundle<'params> {
    pub fn new() -> Self {
        Self::default()
    }

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
    /// [`Done::Close`](crate::thread::Done::Close).
    ///
    /// **Race resolution:** if multiple threads error nearly
    /// simultaneously, exactly one wins the mutex/take and drains
    /// the callback list. Which thread "wins" is non-deterministic.
    /// Callbacks run on the winning thread, same constraints as
    /// [`ThreadStream::on_done`](crate::thread::ThreadStream::on_done)
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
        first_error::inject(&mut self.threads, std::mem::take(&mut self.on_first_error));

        let entries = self
            .threads
            .into_iter()
            .map(|t| {
                let thread_name = t.thread_name();
                let handle = scope.spawn(move || t.run());
                BundleEntry {
                    handle,
                    thread_name,
                }
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
        let joins: Vec<Join> = self
            .entries
            .into_iter()
            .map(|entry| match entry.handle.join() {
                Ok(Ok(())) => Join::Ok,
                Ok(Err(e)) => Join::Error(e),
                Err(panic_err) => Join::Panic(fatal!(
                    "Thread {} panicked: {:?}",
                    entry.thread_name,
                    panic_err
                )),
            })
            .collect();
        BundleJoin::new(joins)
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{ImmediateClose, Panicker, ThreadA, ThreadB, WorkError};
    use super::ThreadBundle;
    use crate::graph::Add;
    use crate::thread::{Join, ThreadStream};

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
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(WorkError::<ThreadA>::new())).unwrap();

        let mut thread_b = ThreadStream::<'_, ThreadB>::new();
        thread_b
            .add(Box::new(ImmediateClose::<ThreadB>::new()))
            .unwrap();

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
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

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
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

        let mut thread_b = ThreadStream::<'_, ThreadB>::new();
        thread_b.add(Box::new(Panicker::<ThreadB>::new())).unwrap();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        std::thread::scope(|s| {
            let results = bundle.start(s).join();
            assert!(matches!(results[0], Join::Panic(_)));
            assert!(matches!(results[1], Join::Panic(_)));
        });
    }
}
