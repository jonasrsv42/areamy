//! Thread-per-subgraph execution with compile-time state guarantees.
//!
//! [`ThreadStream`] schedules a subgraph onto a dedicated OS thread.
//! Starting a thread returns a [`ThreadStreamHandle`] which can be joined.

use crate::error::{Error, ErrorKind};
use crate::{ThreadId, Workable, fatal, graph::Add};
use std::thread::{JoinHandle, spawn};

/// Shorthand for workable to make types more readable in this file.
type Workables<ThreadIdType> = Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>;

/// [`JoinError`] allows us to error in a separate thread and still return
/// the workables for possible recovery.
struct JoinError<ThreadIdType: ThreadId> {
    error: Error,
    workables: Workables<ThreadIdType>,
}

/// Run loop that works all workables until one returns an error.
/// Returns Ok(()) on [ErrorKind::Closed], propagates other errors.
fn work_loop<ThreadIdType: ThreadId>(workables: &mut Workables<ThreadIdType>) -> Result<(), Error> {
    loop {
        for workable in workables.iter_mut() {
            if let Err(error) = workable.work() {
                match error.kind {
                    ErrorKind::Closed => return Ok(()),
                    _ => {
                        #[cfg(not(feature = "silent"))]
                        eprintln!(
                            "In thread {} error: {}",
                            std::any::type_name::<ThreadIdType>(),
                            error
                        );

                        return Err(error);
                    }
                }
            }
        }
    }
}

/// Runs the work loop and returns workables (for restart) or error with workables.
fn run<ThreadIdType: ThreadId>(
    mut workables: Workables<ThreadIdType>,
) -> Result<Workables<ThreadIdType>, JoinError<ThreadIdType>> {
    if workables.is_empty() {
        return Ok(workables);
    }

    match work_loop::<ThreadIdType>(&mut workables) {
        Ok(()) => Ok(workables),
        Err(error) => Err(JoinError { error, workables }),
    }
}

/// An idle thread that can have workables added and be started.
///
/// Use [`Add`] to add workables, then call [`start`](Self::start) to
/// spawn the OS thread and get a [`ThreadStreamHandle`].
///
/// # Example
///
/// ```ignore
/// let mut thread = ThreadStream::<MyThread>::new();
/// make_work::<_, MyThread>(some_vertex, &mut thread)?;
/// let handle = thread.start();  // consumes ThreadStream, returns handle
/// ```
pub struct ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    workables: Workables<ThreadIdType>,
}

/// A handle to a running thread, returned by [`ThreadStream::start`].
///
/// Call [`join`](Self::join) to wait for the thread to complete and
/// get the [`ThreadStream`] back for potential restart.
pub struct ThreadStreamHandle<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    thread: JoinHandle<Result<Workables<ThreadIdType>, JoinError<ThreadIdType>>>,
}

impl<ThreadIdType> Default for ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    fn default() -> Self {
        Self {
            workables: Vec::new(),
        }
    }
}

impl<ThreadIdType> ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    /// Create a new idle thread stream with no workables.
    pub fn new() -> Self {
        Self::default()
    }

    /// Start the thread, consuming this [`ThreadStream`] and returning a handle.
    ///
    /// The thread will run until a workable returns [`ErrorKind::Closed`]
    /// (clean exit) or another error (failure).
    pub fn start(self) -> ThreadStreamHandle<ThreadIdType> {
        ThreadStreamHandle {
            thread: spawn(move || run(self.workables)),
        }
    }
}

impl<ThreadIdType> ThreadStreamHandle<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    /// Join the thread, waiting for it to complete.
    ///
    /// # Returns
    ///
    /// - `Ok((stream, None))` - clean exit, restartable
    /// - `Ok((stream, Some(err)))` - work error, still restartable
    /// - `Err(err)` - thread panicked, workables lost, not restartable
    pub fn join(self) -> Result<(ThreadStream<ThreadIdType>, Option<Error>), Error> {
        match self.thread.join() {
            Ok(Ok(workables)) => Ok((ThreadStream { workables }, None)),
            Ok(Err(join_err)) => Ok((
                ThreadStream {
                    workables: join_err.workables,
                },
                Some(join_err.error),
            )),
            Err(panic_err) => Err(fatal!(
                "Thread {} panicked: {:?}",
                std::any::type_name::<ThreadIdType>(),
                panic_err
            )),
        }
    }
}

/// Add workables to an idle thread stream.
impl<ThreadIdType: ThreadId> Add<dyn Workable<ThreadId = ThreadIdType>>
    for ThreadStream<ThreadIdType>
{
    fn add(&mut self, workable: Box<dyn Workable<ThreadId = ThreadIdType>>) -> Result<(), Error> {
        self.workables.push(workable);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::marker::Connection;
    use crate::{closed, fatal};

    #[derive(Debug, Clone)]
    struct TestThread;
    impl ThreadId for TestThread {}

    struct ImmediateClose;
    impl Connection for ImmediateClose {}

    impl Workable for ImmediateClose {
        type ThreadId = TestThread;
        fn work(&mut self) -> Result<(), Error> {
            Err(closed!())
        }
    }

    struct WorkError;
    impl Connection for WorkError {}

    impl Workable for WorkError {
        type ThreadId = TestThread;
        fn work(&mut self) -> Result<(), Error> {
            Err(fatal!("work failed"))
        }
    }

    struct Panicker;
    impl Connection for Panicker {}

    impl Workable for Panicker {
        type ThreadId = TestThread;
        fn work(&mut self) -> Result<(), Error> {
            panic!("intentional panic for testing");
        }
    }

    #[test]
    fn start_and_join_empty_thread() {
        let thread = ThreadStream::<TestThread>::new();
        let handle = thread.start();
        let (recovered, err) = handle.join().unwrap();
        assert!(err.is_none());
        // Can restart
        let _handle2 = recovered.start();
    }

    #[test]
    fn start_and_join_with_workable() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(ImmediateClose)).unwrap();
        let handle = thread.start();
        let (recovered, err) = handle.join().unwrap();
        assert!(err.is_none()); // Closed is clean exit
        // Workable is recovered
        assert_eq!(recovered.workables.len(), 1);
    }

    #[test]
    fn work_error_is_recoverable() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(WorkError)).unwrap();
        let handle = thread.start();
        let (recovered, err) = handle.join().unwrap();
        // Work error is reported
        assert!(err.is_some());
        assert!(err.unwrap().to_string().contains("work failed"));
        // But workable is still recovered - can restart
        assert_eq!(recovered.workables.len(), 1);
    }

    #[test]
    fn panic_loses_thread() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(Panicker)).unwrap();
        let handle = thread.start();
        let result = handle.join();
        // Panic returns Err, thread is lost
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("panicked"));
    }

    #[test]
    fn restart_after_clean_exit() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(ImmediateClose)).unwrap();

        // First run
        let handle = thread.start();
        let (thread, _) = handle.join().unwrap();

        // Second run - same thread, same workable
        let handle = thread.start();
        let (recovered, _) = handle.join().unwrap();
        assert_eq!(recovered.workables.len(), 1);
    }
}
