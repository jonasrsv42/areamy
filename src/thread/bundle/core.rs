//! [`ThreadBundle`] and [`ThreadBundleHandle`] — the user-facing
//! bundle API.

use super::first_error::{self, OnFirstError};
use crate::thread::done::Failure;
use crate::thread::join::BundleJoin;
use crate::thread::type_erase::{
    TypeErasedInternalThreadStream, TypeErasedInternalThreadStreamHandle,
};

/// A bundle of idle threads that can be started together.
#[derive(Default)]
pub struct ThreadBundle {
    threads: Vec<Box<dyn TypeErasedInternalThreadStream>>,
    on_first_error: Vec<OnFirstError>,
}

/// A handle to running threads, returned by [`ThreadBundle::start`].
pub struct ThreadBundleHandle {
    threads: Vec<Box<dyn TypeErasedInternalThreadStreamHandle>>,
}

impl ThreadBundle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, thread: impl TypeErasedInternalThreadStream + 'static) -> &mut Self {
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

    pub fn start(mut self) -> ThreadBundleHandle {
        first_error::inject(&mut self.threads, std::mem::take(&mut self.on_first_error));

        ThreadBundleHandle {
            threads: self.threads.into_iter().map(|t| t.start()).collect(),
        }
    }
}

impl ThreadBundleHandle {
    /// Join all threads. Returns one [`Join`](crate::thread::Join)
    /// per registered thread, in registration order, wrapped in
    /// [`BundleJoin`].
    pub fn join(self) -> BundleJoin {
        BundleJoin::new(self.threads.into_iter().map(|t| t.join()).collect())
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
        let handle = bundle.start();
        assert!(handle.join().is_empty());
    }

    #[test]
    fn heterogeneous_threads() {
        let mut bundle = ThreadBundle::new();
        bundle
            .add(ThreadStream::<ThreadA>::new())
            .add(ThreadStream::<ThreadB>::new());
        let handle = bundle.start();
        let results = handle.join();
        assert_eq!(results.len(), 2);
        assert!(matches!(results[0], Join::Ok));
        assert!(matches!(results[1], Join::Ok));
    }

    #[test]
    fn chaining() {
        let mut bundle = ThreadBundle::new();
        bundle
            .add(ThreadStream::<ThreadA>::new())
            .add(ThreadStream::<ThreadA>::new())
            .add(ThreadStream::<ThreadB>::new());
        assert_eq!(bundle.threads.len(), 3);
    }

    #[test]
    fn work_error_appears_in_results() {
        let mut thread_a = ThreadStream::<ThreadA>::new();
        thread_a.add(Box::new(WorkError::<ThreadA>::new())).unwrap();

        let mut thread_b = ThreadStream::<ThreadB>::new();
        thread_b
            .add(Box::new(ImmediateClose::<ThreadB>::new()))
            .unwrap();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        let results = bundle.start().join();
        assert!(matches!(results[0], Join::Error(_)));
        assert!(matches!(results[1], Join::Ok));
    }

    #[test]
    fn panic_appears_as_panic_variant() {
        let mut thread_a = ThreadStream::<ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

        let thread_b = ThreadStream::<ThreadB>::new();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        let results = bundle.start().join();
        assert!(matches!(results[0], Join::Panic(_)));
        assert!(matches!(results[1], Join::Ok));
    }

    #[test]
    fn multiple_panics_each_recorded() {
        let mut thread_a = ThreadStream::<ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

        let mut thread_b = ThreadStream::<ThreadB>::new();
        thread_b.add(Box::new(Panicker::<ThreadB>::new())).unwrap();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        let results = bundle.start().join();
        assert!(matches!(results[0], Join::Panic(_)));
        assert!(matches!(results[1], Join::Panic(_)));
    }
}
