//! Thread bundle for managing multiple heterogeneous threads as a unit.

use super::{ThreadId, ThreadStream, ThreadStreamHandle};
use crate::any_err;
use crate::error::{AnyErr, Error};
use std::fmt;

/// Error returned when one or more threads in a bundle panicked.
#[derive(Debug)]
pub struct BundlePanics(pub Vec<Error>);

impl fmt::Display for BundlePanics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} thread(s) panicked in bundle", self.0.len())
    }
}

impl std::error::Error for BundlePanics {}

impl AnyErr for BundlePanics {}

impl From<BundlePanics> for Box<dyn AnyErr> {
    fn from(err: BundlePanics) -> Self {
        Box::new(err)
    }
}

/// Internal trait for type-erased idle threads.
trait TypeErasedInternalThreadStream: Send {
    fn start(self: Box<Self>) -> Box<dyn TypeErasedInternalThreadStreamHandle>;
}

/// Internal trait for type-erased running threads.
trait TypeErasedInternalThreadStreamHandle: Send {
    fn join(
        self: Box<Self>,
    ) -> Result<(Box<dyn TypeErasedInternalThreadStream>, Option<Error>), Error>;
}

impl<ThreadIdType: ThreadId + 'static> TypeErasedInternalThreadStream
    for ThreadStream<ThreadIdType>
{
    fn start(self: Box<Self>) -> Box<dyn TypeErasedInternalThreadStreamHandle> {
        Box::new((*self).start())
    }
}

impl<ThreadIdType: ThreadId + 'static> TypeErasedInternalThreadStreamHandle
    for ThreadStreamHandle<ThreadIdType>
{
    fn join(
        self: Box<Self>,
    ) -> Result<(Box<dyn TypeErasedInternalThreadStream>, Option<Error>), Error> {
        let (stream, err) = (*self).join()?;
        Ok((Box::new(stream), err))
    }
}

/// A bundle of idle threads that can be started together.
#[derive(Default)]
pub struct ThreadBundle {
    threads: Vec<Box<dyn TypeErasedInternalThreadStream>>,
}

/// A handle to running threads, returned by [`ThreadBundle::start`].
pub struct ThreadBundleHandle {
    threads: Vec<Box<dyn TypeErasedInternalThreadStreamHandle>>,
}

impl ThreadBundle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add<ThreadIdType: ThreadId + 'static>(
        &mut self,
        thread: ThreadStream<ThreadIdType>,
    ) -> &mut Self {
        self.threads.push(Box::new(thread));
        self
    }

    pub fn start(self) -> ThreadBundleHandle {
        ThreadBundleHandle {
            threads: self.threads.into_iter().map(|t| t.start()).collect(),
        }
    }
}

impl ThreadBundleHandle {
    /// Join all threads.
    ///
    /// # Returns
    ///
    /// - `Ok((bundle, errors))` - all threads joined, bundle is restartable
    /// - `Err(panics)` - one or more threads panicked, bundle is lost
    pub fn join(self) -> Result<(ThreadBundle, Vec<Error>), Error> {
        let mut recovered = Vec::new();
        let mut errors = Vec::new();
        let mut panics = Vec::new();

        for thread in self.threads {
            match thread.join() {
                Ok((idle, err)) => {
                    recovered.push(idle);
                    if let Some(err) = err {
                        errors.push(err);
                    }
                }
                Err(err) => {
                    panics.push(err);
                }
            }
        }

        if panics.is_empty() {
            Ok((ThreadBundle { threads: recovered }, errors))
        } else {
            Err(any_err!(BundlePanics(panics)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::marker::Connection;
    use crate::graph::Add;
    use crate::{Workable, closed, fatal};

    #[derive(Debug, Clone)]
    struct ThreadA;
    impl ThreadId for ThreadA {}

    #[derive(Debug, Clone)]
    struct ThreadB;
    impl ThreadId for ThreadB {}

    struct ImmediateClose<T>(std::marker::PhantomData<T>);
    impl<T> ImmediateClose<T> {
        fn new() -> Self {
            Self(std::marker::PhantomData)
        }
    }
    impl<T> Connection for ImmediateClose<T> {}

    impl<T: ThreadId> Workable for ImmediateClose<T> {
        type ThreadId = T;
        fn work(&mut self) -> Result<(), Error> {
            Err(closed!())
        }
    }

    struct WorkError<T>(std::marker::PhantomData<T>);
    impl<T> WorkError<T> {
        fn new() -> Self {
            Self(std::marker::PhantomData)
        }
    }
    impl<T> Connection for WorkError<T> {}

    impl<T: ThreadId> Workable for WorkError<T> {
        type ThreadId = T;
        fn work(&mut self) -> Result<(), Error> {
            Err(fatal!("work failed"))
        }
    }

    struct Panicker<T>(std::marker::PhantomData<T>);
    impl<T> Panicker<T> {
        fn new() -> Self {
            Self(std::marker::PhantomData)
        }
    }
    impl<T> Connection for Panicker<T> {}

    impl<T: ThreadId> Workable for Panicker<T> {
        type ThreadId = T;
        fn work(&mut self) -> Result<(), Error> {
            panic!("intentional panic");
        }
    }

    #[test]
    fn bundle_start_and_join_empty() {
        let bundle = ThreadBundle::new();
        let handle = bundle.start();
        let (recovered, errors) = handle.join().unwrap();
        assert!(errors.is_empty());
        // Can restart
        let _handle2 = recovered.start();
    }

    #[test]
    fn bundle_with_heterogeneous_threads() {
        let mut bundle = ThreadBundle::new();
        bundle
            .add(ThreadStream::<ThreadA>::new())
            .add(ThreadStream::<ThreadB>::new());
        let handle = bundle.start();
        let (recovered, errors) = handle.join().unwrap();
        assert!(errors.is_empty());
        // Both threads recovered
        assert_eq!(recovered.threads.len(), 2);
    }

    #[test]
    fn bundle_chaining() {
        let mut bundle = ThreadBundle::new();
        bundle
            .add(ThreadStream::<ThreadA>::new())
            .add(ThreadStream::<ThreadA>::new())
            .add(ThreadStream::<ThreadB>::new());
        assert_eq!(bundle.threads.len(), 3);
    }

    #[test]
    fn bundle_work_error_is_recoverable() {
        let mut thread_a = ThreadStream::<ThreadA>::new();
        thread_a.add(Box::new(WorkError::<ThreadA>::new())).unwrap();

        let mut thread_b = ThreadStream::<ThreadB>::new();
        thread_b
            .add(Box::new(ImmediateClose::<ThreadB>::new()))
            .unwrap();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        let handle = bundle.start();
        let (recovered, errors) = handle.join().unwrap();

        // One work error collected
        assert_eq!(errors.len(), 1);
        // Both threads recovered
        assert_eq!(recovered.threads.len(), 2);
    }

    #[test]
    fn bundle_panic_loses_bundle() {
        let mut thread_a = ThreadStream::<ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

        let thread_b = ThreadStream::<ThreadB>::new();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        let handle = bundle.start();
        let result = handle.join();

        // Panic returns Err
        assert!(result.is_err());
        let err = result.err().unwrap();
        // Can downcast to BundlePanics
        assert!(err.is::<BundlePanics>());
    }

    #[test]
    fn bundle_multiple_panics_collected() {
        let mut thread_a = ThreadStream::<ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

        let mut thread_b = ThreadStream::<ThreadB>::new();
        thread_b.add(Box::new(Panicker::<ThreadB>::new())).unwrap();

        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b);

        let handle = bundle.start();
        let result = handle.join();

        assert!(result.is_err());
        let err = result.err().unwrap();
        // Downcast to get panic count
        let panics = err.downcast_any::<BundlePanics>().unwrap();
        assert_eq!(panics.0.len(), 2);
    }

    #[test]
    fn bundle_restart_after_success() {
        let mut bundle = ThreadBundle::new();
        bundle
            .add(ThreadStream::<ThreadA>::new())
            .add(ThreadStream::<ThreadB>::new());

        // First run
        let handle = bundle.start();
        let (bundle, _) = handle.join().unwrap();

        // Second run
        let handle = bundle.start();
        let (recovered, _) = handle.join().unwrap();
        assert_eq!(recovered.threads.len(), 2);
    }
}
