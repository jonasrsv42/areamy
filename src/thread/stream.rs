//! Thread-per-subgraph execution with compile-time state guarantees.
//!
//! [`ThreadStream`] schedules a subgraph onto a dedicated OS thread.
//! Starting a thread returns a [`ThreadStreamHandle`] which can be
//! joined.
//!
//! Workables are dropped inside the spawn closure as soon as the
//! work loop returns. This means dropping the last `Sender` /
//! `Receiver` an exiting thread holds fires the close cascade *before*
//! the OS thread exits — sibling threads sharing edges with the dying
//! thread see `Closed` immediately, instead of waiting until the
//! caller reaches this thread in a sequential `bundle.join()`.

use super::callback::{self, OnDone, PanicGuard};
use super::done::Done;
use super::join::Join;
use crate::error::{Error, ErrorKind};
use crate::{ThreadId, Workable, fatal, graph::Add};
use std::thread::{JoinHandle, spawn};

type Workables<ThreadIdType> = Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>;

/// Drive workables until the vec drains. A workable returning
/// [`ErrorKind::Closed`] is dropped immediately — its outgoing
/// `Sender`/`Receiver` handles drop with it and the close-on-drop
/// cascade fires within this work loop, so other workables in the
/// same thread that share edges with it observe `Closed` on their
/// next poll.
fn work_loop<ThreadIdType: ThreadId>(workables: &mut Workables<ThreadIdType>) -> Result<(), Error> {
    while !workables.is_empty() {
        let mut i = 0;
        while i < workables.len() {
            match workables[i].work() {
                Ok(()) => i += 1,
                Err(error) if matches!(error.kind, ErrorKind::Closed) => {
                    // Drop the closed workable here — fires cascade.
                    // swap_remove is O(1) and the iteration order
                    // doesn't matter for fairness.
                    workables.swap_remove(i);
                }
                Err(error) => {
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
    Ok(())
}

/// Run the work loop. Workables are dropped here at end of scope,
/// before the OS thread exits — so close-on-drop cascades fire as
/// part of thread termination. Empty input falls out of `work_loop`'s
/// outer `while` immediately, so no explicit guard is needed.
fn run<ThreadIdType: ThreadId>(mut workables: Workables<ThreadIdType>) -> Result<(), Error> {
    work_loop::<ThreadIdType>(&mut workables)
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
    on_done: Vec<OnDone>,
}

/// A handle to a running thread, returned by [`ThreadStream::start`].
///
/// Call [`join`](Self::join) to wait for the thread to complete.
pub struct ThreadStreamHandle {
    thread: JoinHandle<Result<(), Error>>,
    /// Captured `type_name::<ThreadIdType>()` from start time, used
    /// to label the thread in panic diagnostics.
    thread_name: &'static str,
}

impl<ThreadIdType> Default for ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    fn default() -> Self {
        Self {
            workables: Vec::new(),
            on_done: Vec::new(),
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

    /// Register a callback to fire when the thread exits.
    ///
    /// Each registered callback fires exactly once, on any exit path
    /// (`Done::Close`, `Done::Error`, or `Done::Panic`), in
    /// registration order, from the dying thread. By the time the
    /// callback runs the workables have already dropped, so this
    /// thread's outgoing edges have closed.
    ///
    /// # Callback constraints
    ///
    /// - Must complete in bounded time. Pushing to an unbounded
    ///   channel (`std::sync::mpsc::channel`) or setting an atomic is
    ///   safe; bounded channels are unsafe because they block when
    ///   full.
    /// - Must not acquire a lock held by code that waits on this
    ///   thread.
    /// - Should not panic. A panic is caught via
    ///   [`catch_unwind`](std::panic::catch_unwind) so it cannot
    ///   propagate, abort the process, or stop later callbacks in
    ///   this thread from running — but the callback's effect is
    ///   silently dropped (logged unless the `silent` feature is
    ///   set).
    /// - The `&Done` / inner `&Error` references are valid only for
    ///   the duration of the call; clone fields to retain anything.
    pub fn on_done<F>(&mut self, callback: F) -> &mut Self
    where
        F: FnOnce(&Done) + Send + 'static,
    {
        self.on_done.push(Box::new(callback));
        self
    }

    /// Start the thread, consuming this [`ThreadStream`] and returning a handle.
    ///
    /// The thread will run until a workable returns [`ErrorKind::Closed`]
    /// (clean exit) or another error (failure).
    pub fn start(self) -> ThreadStreamHandle {
        let Self { workables, on_done } = self;
        ThreadStreamHandle {
            thread: spawn(move || {
                let mut guard = PanicGuard::new(on_done);
                let result = run::<ThreadIdType>(workables);
                let callbacks = guard.drain();
                let done = match &result {
                    Ok(()) => Done::Close,
                    Err(e) => Done::Error(e),
                };
                callback::fire(callbacks, &done);
                result
            }),
            thread_name: std::any::type_name::<ThreadIdType>(),
        }
    }
}

impl ThreadStreamHandle {
    /// Join the thread, waiting for it to complete.
    pub fn join(self) -> Join {
        match self.thread.join() {
            Ok(Ok(())) => Join::Ok,
            Ok(Err(e)) => Join::Error(e),
            Err(panic_err) => Join::Panic(fatal!(
                "Thread {} panicked: {:?}",
                self.thread_name,
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
        assert!(matches!(handle.join(), Join::Ok));
    }

    #[test]
    fn closed_workable_exits_cleanly() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(ImmediateClose)).unwrap();
        let handle = thread.start();
        assert!(matches!(handle.join(), Join::Ok));
    }

    #[test]
    fn work_error_is_reported() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(WorkError)).unwrap();
        let handle = thread.start();
        match handle.join() {
            Join::Error(e) => assert!(e.to_string().contains("work failed")),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn panic_is_reported_as_panic_variant() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(Panicker)).unwrap();
        let handle = thread.start();
        match handle.join() {
            Join::Panic(e) => assert!(e.to_string().contains("panicked")),
            other => panic!("expected Panic, got {:?}", other),
        }
    }

    // ---- on_done callback coverage ----

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Records what kind of `Done` the callback observed.
    #[derive(Debug, PartialEq)]
    enum Observed {
        Close,
        Error(String),
        Panic,
    }

    fn record(seen: Arc<Mutex<Vec<Observed>>>) -> impl FnOnce(&Done) + Send + 'static {
        move |done| {
            let obs = match done {
                Done::Close => Observed::Close,
                Done::Error(e) => Observed::Error(e.to_string()),
                Done::Panic => Observed::Panic,
            };
            seen.lock().unwrap().push(obs);
        }
    }

    #[test]
    fn on_done_fires_close_on_clean_exit() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(ImmediateClose)).unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        thread.on_done(record(seen.clone()));

        let _ = thread.start().join();
        assert_eq!(*seen.lock().unwrap(), vec![Observed::Close]);
    }

    #[test]
    fn on_done_fires_error_on_work_failure() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(WorkError)).unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        thread.on_done(record(seen.clone()));

        let _ = thread.start().join();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        match &seen[0] {
            Observed::Error(msg) => assert!(msg.contains("work failed")),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn on_done_fires_panic_on_panic() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(Panicker)).unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        thread.on_done(record(seen.clone()));

        let _ = thread.start().join();
        assert_eq!(*seen.lock().unwrap(), vec![Observed::Panic]);
    }

    #[test]
    fn on_done_multiple_callbacks_fire_in_registration_order() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(ImmediateClose)).unwrap();

        let order = Arc::new(Mutex::new(Vec::<u32>::new()));
        let o1 = order.clone();
        let o2 = order.clone();
        let o3 = order.clone();
        thread.on_done(move |_| o1.lock().unwrap().push(1));
        thread.on_done(move |_| o2.lock().unwrap().push(2));
        thread.on_done(move |_| o3.lock().unwrap().push(3));

        let _ = thread.start().join();
        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn on_done_no_callback_when_none_registered() {
        // Sanity: the bookkeeping path works when nothing's registered.
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(ImmediateClose)).unwrap();
        assert!(matches!(thread.start().join(), Join::Ok));
    }

    /// A workable that holds a sentinel which records its drop order
    /// — used to verify that callbacks fire *after* workables drop.
    struct DropMarker {
        marker: Arc<AtomicUsize>,
        counter: Arc<AtomicUsize>,
    }
    impl Connection for DropMarker {}
    impl Workable for DropMarker {
        type ThreadId = TestThread;
        fn work(&mut self) -> Result<(), Error> {
            Err(closed!())
        }
    }
    impl Drop for DropMarker {
        fn drop(&mut self) {
            // Stamp this marker with a monotonically increasing tick.
            let tick = self.counter.fetch_add(1, Ordering::SeqCst);
            self.marker.store(tick + 1, Ordering::SeqCst);
        }
    }

    /// A panicking on_done callback during clean exit must not
    /// propagate (would convert Join::Ok to Join::Panic) and must not
    /// prevent later callbacks from running.
    #[test]
    fn on_done_panic_on_normal_path_is_isolated() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(ImmediateClose)).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s1 = seen.clone();
        let s3 = seen.clone();
        thread.on_done(move |_| s1.lock().unwrap().push(1));
        thread.on_done(|_| panic!("callback 2 panics"));
        thread.on_done(move |_| s3.lock().unwrap().push(3));

        assert!(matches!(thread.start().join(), Join::Ok));
        assert_eq!(*seen.lock().unwrap(), vec![1, 3]);
    }

    /// A panicking on_done callback fired from the panic path must
    /// not double-panic (which would abort the process) and must not
    /// prevent later callbacks from running.
    #[test]
    fn on_done_panic_on_panic_path_is_isolated() {
        let mut thread = ThreadStream::<TestThread>::new();
        thread.add(Box::new(Panicker)).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s1 = seen.clone();
        let s3 = seen.clone();
        thread.on_done(move |_| s1.lock().unwrap().push(1));
        thread.on_done(|_| panic!("callback 2 panics during panic"));
        thread.on_done(move |_| s3.lock().unwrap().push(3));

        // If the catch_unwind in PanicGuard::drop were missing, the
        // double-panic would abort and this assert would never run.
        match thread.start().join() {
            Join::Panic(_) => {}
            other => panic!("expected Join::Panic, got {:?}", other),
        }
        assert_eq!(*seen.lock().unwrap(), vec![1, 3]);
    }

    #[test]
    fn on_done_fires_after_workables_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let workable_drop_tick = Arc::new(AtomicUsize::new(0));
        let callback_tick = Arc::new(AtomicUsize::new(0));

        let mut thread = ThreadStream::<TestThread>::new();
        thread
            .add(Box::new(DropMarker {
                marker: workable_drop_tick.clone(),
                counter: counter.clone(),
            }))
            .unwrap();

        let cb_tick = callback_tick.clone();
        let cb_counter = counter.clone();
        thread.on_done(move |_| {
            let tick = cb_counter.fetch_add(1, Ordering::SeqCst);
            cb_tick.store(tick + 1, Ordering::SeqCst);
        });

        let _ = thread.start().join();
        let w = workable_drop_tick.load(Ordering::SeqCst);
        let c = callback_tick.load(Ordering::SeqCst);
        assert!(
            w > 0 && c > w,
            "workable drop ({}) must precede callback ({})",
            w,
            c
        );
    }
}
