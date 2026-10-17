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

use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::Connection;
use crate::thread::callback::{self, InitGuard, OnDone};
use crate::thread::done::Done;
use crate::thread::join::{Join, Spawned};
use crate::thread::{Spawn, ThreadId};
use crate::work::Workable;
use crate::work::work_each;
use std::thread::Scope;

type Workables<'params, ThreadIdType> = Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>;

/// Drive workables until the vec drains. A workable returning
/// [`crate::error::ErrorKind::Closed`] is dropped immediately — its outgoing
/// `Sender`/`Receiver` handles drop with it and the close-on-drop
/// cascade fires within this work loop, so other workables in the
/// same thread that share edges with it observe `Closed` on their
/// next poll.
#[cfg_attr(feature = "silent", allow(unused_variables))]
fn work_loop<'params, ThreadIdType: ThreadId>(
    workables: &mut Workables<'params, ThreadIdType>,
) -> Result<(), Error> {
    while !workables.is_empty() {
        work_each(workables).inspect_err(|error| {
            #[cfg(not(feature = "silent"))]
            eprintln!("In thread {} error: {}", ThreadIdType::name(), error);
        })?;
    }
    Ok(())
}

/// Run the work loop. Workables are dropped here at end of scope,
/// before the OS thread exits — so close-on-drop cascades fire as
/// part of thread termination. Empty input falls out of `work_loop`'s
/// outer `while` immediately, so no explicit guard is needed.
fn run<'params, ThreadIdType: ThreadId>(
    mut workables: Workables<'params, ThreadIdType>,
) -> Result<(), Error> {
    work_loop(&mut workables)
}

/// An idle thread that can have workables added and be started.
///
/// Use [`Streams::stream`] for a single root or [`Add`] to add workables, then call
/// [`start`](Self::start) to spawn the OS thread and get a [`ThreadStreamHandle`].
///
/// # Example
///
/// ```ignore
/// thread_id!(MyThread);
///
/// let thread = root.stream::<MyThread>();
/// std::thread::scope(|s| {
///     let handle = thread.start(s);  // consumes ThreadStream, returns handle
///     // ...
/// });
/// ```
#[must_use = "start the thread or add it to a bundle; dropping it drops the subgraph"]
pub struct ThreadStream<'params, ThreadIdType>
where
    ThreadIdType: ThreadId,
{
    pub(crate) workables: Workables<'params, ThreadIdType>,
    pub(crate) on_done: Vec<OnDone>,
}

/// [`Streams`] puts a root on its own thread: `threads.add(root.stream::<EncoderThread>())`.
///
/// The thread is an optional annotation when the root's type already fixes it.
pub trait Streams<'params>: Sized {
    /// Moves this root onto a thread of its own.
    fn stream<ThreadIdType>(self) -> ThreadStream<'params, ThreadIdType>
    where
        ThreadIdType: ThreadId,
        Self: Workable<ThreadId = ThreadIdType> + 'params;
}

impl<'params, NodeType: Connection> Streams<'params> for NodeType {
    fn stream<ThreadIdType>(self) -> ThreadStream<'params, ThreadIdType>
    where
        ThreadIdType: ThreadId,
        Self: Workable<ThreadId = ThreadIdType> + 'params,
    {
        ThreadStream {
            workables: vec![Box::new(self)],
            on_done: Vec::new(),
        }
    }
}

/// A handle to a running thread, returned by [`ThreadStream::start`].
///
/// Call [`join`](Self::join) to wait for the thread to complete.
pub struct ThreadStreamHandle<'threads> {
    spawned: Spawned<'threads>,
}

impl<'params, ThreadIdType> Default for ThreadStream<'params, ThreadIdType>
where
    ThreadIdType: ThreadId,
{
    fn default() -> Self {
        Self {
            workables: Vec::new(),
            on_done: Vec::new(),
        }
    }
}

impl<'params, ThreadIdType> ThreadStream<'params, ThreadIdType>
where
    ThreadIdType: ThreadId,
{
    /// Create a new idle thread stream with no workables.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a callback to fire when the thread exits.
    ///
    /// Each registered callback fires exactly once, on any exit path
    /// (`Done::Close`, `Done::Error`, or `Done::Panic`; not under
    /// `panic = "abort"`, where a panic ends the process), in
    /// registration order, from the dying thread. By the time the
    /// callback runs the workables have already dropped, so this
    /// thread's outgoing edges have closed.
    ///
    /// A thread the OS refused to spawn exits too: its callbacks fire
    /// with a [`NeverStarted`](crate::thread::NeverStarted)
    /// `Done::Error`, synchronously inside `start`, so a lock the
    /// caller holds across `start` must not be taken here. It carries
    /// no reason; `join` returns the same error with the OS's reason,
    /// as `Join::Error`.
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

    /// The work loop as a task for any thread. Its exit guard exists from here on, so `on_done`
    /// fires even if the task is dropped unrun because its spawn was refused.
    pub(crate) fn into_task(self) -> impl FnOnce() -> Result<(), Error> + Send + 'params {
        /// One capture, so drop order is the fields': a task dropped unrun drops its workables
        /// (closing their edges) before the guard fires `on_done`, as on every other exit.
        struct Unrun<'params, ThreadIdType: ThreadId> {
            workables: Workables<'params, ThreadIdType>,
            guard: InitGuard,
        }
        let Self { workables, on_done } = self;
        let unrun = Unrun {
            workables,
            guard: InitGuard::new(ThreadIdType::name(), on_done),
        };
        move || {
            let Unrun { workables, guard } = unrun;
            let guard = guard.start();
            let result = run(workables);
            let callbacks = guard.drain();
            let done = match &result {
                Ok(()) => Done::Close,
                Err(e) => Done::Error(e),
            };
            callback::fire(callbacks, &done);
            result
        }
    }

    /// Start the thread, consuming this [`ThreadStream`] and returning a handle.
    ///
    /// Spawns into the provided `scope`. The thread will run until a
    /// workable returns [`ErrorKind::Closed`](crate::error::ErrorKind::Closed) (clean exit) or another
    /// error (failure).
    pub fn start<'threads>(
        self,
        scope: &'threads Scope<'threads, 'params>,
    ) -> ThreadStreamHandle<'threads>
    where
        'params: 'threads,
    {
        self.with().start(scope)
    }

    /// Options for its own OS thread, such as [Spawn::stack_size]. The [Spawn] can only be started
    /// or added to a bundle, never run here, so the options always take effect.
    pub fn with(self) -> Spawn<Self> {
        Spawn::new(self)
    }
}

impl<'params, ThreadIdType: ThreadId> Spawn<ThreadStream<'params, ThreadIdType>> {
    /// Start on its own OS thread, spawned into `scope` with these options.
    pub fn start<'threads>(
        self,
        scope: &'threads Scope<'threads, 'params>,
    ) -> ThreadStreamHandle<'threads>
    where
        'params: 'threads,
    {
        let Spawn { thread, options } = self;
        ThreadStreamHandle {
            spawned: options.spawn(scope, ThreadIdType::name(), thread.into_task()),
        }
    }
}

impl<'threads> ThreadStreamHandle<'threads> {
    /// Join the thread, waiting for it to complete.
    pub fn join(self) -> Join {
        self.spawned.join()
    }
}

/// Add workables to an idle thread stream.
impl<'params, ThreadIdType: ThreadId> Add<dyn Workable<ThreadId = ThreadIdType> + 'params>
    for ThreadStream<'params, ThreadIdType>
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    ) -> Result<(), Error> {
        self.workables.push(workable);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::closed;
    use crate::thread::fixtures::{ImmediateClose, Panicker, WorkError};

    crate::thread_id!(TestThread);

    #[test]
    fn start_and_join_empty_thread() {
        let thread = ThreadStream::<'_, TestThread>::new();
        std::thread::scope(|s| {
            let handle = thread.start(s);
            assert!(matches!(handle.join(), Join::Ok));
        });
    }

    #[test]
    fn closed_workable_exits_cleanly() {
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(ImmediateClose::new())).unwrap();
        std::thread::scope(|s| {
            let handle = thread.start(s);
            assert!(matches!(handle.join(), Join::Ok));
        });
    }

    #[test]
    fn stream_infers_thread_from_root() {
        let thread = ImmediateClose::<TestThread>::new().stream();
        std::thread::scope(|s| {
            let handle = thread.start(s);
            assert!(matches!(handle.join(), Join::Ok));
        });
    }

    crate::thread_id!(MacroThread, pub(crate) OtherMacroThread);

    #[test]
    fn thread_id_macro_declares_thread_ids() {
        let _a = ThreadStream::<'_, MacroThread>::new();
        let _b = ThreadStream::<'_, OtherMacroThread>::new();
    }

    #[test]
    fn work_error_is_reported() {
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(WorkError::new())).unwrap();
        std::thread::scope(|s| {
            let handle = thread.start(s);
            match handle.join() {
                Join::Error(e) => assert!(e.to_string().contains("work failed")),
                other => panic!("expected Error, got {:?}", other),
            }
        });
    }

    #[test]
    fn panic_is_reported_as_panic_variant() {
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(Panicker::new())).unwrap();
        std::thread::scope(|s| {
            let handle = thread.start(s);
            match handle.join() {
                Join::Panic(e) => assert!(e.to_string().contains("panicked")),
                other => panic!("expected Panic, got {:?}", other),
            }
        });
    }

    // ---- on_done callback coverage ----

    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

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
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(ImmediateClose::new())).unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        thread.on_done(record(seen.clone()));

        std::thread::scope(|s| {
            let _ = thread.start(s).join();
        });
        assert_eq!(*seen.lock().unwrap(), vec![Observed::Close]);
    }

    #[test]
    fn on_done_fires_error_on_work_failure() {
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(WorkError::new())).unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        thread.on_done(record(seen.clone()));

        std::thread::scope(|s| {
            let _ = thread.start(s).join();
        });
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        match &seen[0] {
            Observed::Error(msg) => assert!(msg.contains("work failed")),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn on_done_fires_panic_on_panic() {
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(Panicker::new())).unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        thread.on_done(record(seen.clone()));

        std::thread::scope(|s| {
            let _ = thread.start(s).join();
        });
        assert_eq!(*seen.lock().unwrap(), vec![Observed::Panic]);
    }

    #[test]
    fn on_done_multiple_callbacks_fire_in_registration_order() {
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(ImmediateClose::new())).unwrap();

        let order = Arc::new(Mutex::new(Vec::<u32>::new()));
        let o1 = order.clone();
        let o2 = order.clone();
        let o3 = order.clone();
        thread.on_done(move |_| o1.lock().unwrap().push(1));
        thread.on_done(move |_| o2.lock().unwrap().push(2));
        thread.on_done(move |_| o3.lock().unwrap().push(3));

        std::thread::scope(|s| {
            let _ = thread.start(s).join();
        });
        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn on_done_no_callback_when_none_registered() {
        // Sanity: the bookkeeping path works when nothing's registered.
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(ImmediateClose::new())).unwrap();
        std::thread::scope(|s| {
            assert!(matches!(thread.start(s).join(), Join::Ok));
        });
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
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(ImmediateClose::new())).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s1 = seen.clone();
        let s3 = seen.clone();
        thread.on_done(move |_| s1.lock().unwrap().push(1));
        thread.on_done(|_| panic!("callback 2 panics"));
        thread.on_done(move |_| s3.lock().unwrap().push(3));

        std::thread::scope(|s| {
            assert!(matches!(thread.start(s).join(), Join::Ok));
        });
        assert_eq!(*seen.lock().unwrap(), vec![1, 3]);
    }

    /// A panicking on_done callback fired from the panic path must
    /// not double-panic (which would abort the process) and must not
    /// prevent later callbacks from running.
    #[test]
    fn on_done_panic_on_panic_path_is_isolated() {
        let mut thread = ThreadStream::<'_, TestThread>::new();
        thread.add(Box::new(Panicker::new())).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s1 = seen.clone();
        let s3 = seen.clone();
        thread.on_done(move |_| s1.lock().unwrap().push(1));
        thread.on_done(|_| panic!("callback 2 panics during panic"));
        thread.on_done(move |_| s3.lock().unwrap().push(3));

        // If the catch_unwind in PanicGuard::drop were missing, the
        // double-panic would abort and this assert would never run.
        std::thread::scope(|sc| match thread.start(sc).join() {
            Join::Panic(_) => {}
            other => panic!("expected Join::Panic, got {:?}", other),
        });
        assert_eq!(*seen.lock().unwrap(), vec![1, 3]);
    }

    #[test]
    fn on_done_fires_after_workables_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let workable_drop_tick = Arc::new(AtomicUsize::new(0));
        let callback_tick = Arc::new(AtomicUsize::new(0));

        let mut thread = ThreadStream::<'_, TestThread>::new();
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

        std::thread::scope(|s| {
            let _ = thread.start(s).join();
        });
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
