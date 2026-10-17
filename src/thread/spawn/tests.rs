use crate::closed;
use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::Connection;
use crate::poll;
use crate::poll::graph::{Graph, GraphBuilder};
use crate::poll::wakers::ThreadLocalWakerAllocator;
use crate::thread::{Done, Join, NeverStarted, ThreadBundle, ThreadId};
use crate::work::{ThreadStream, Workable};
use alloc::sync::Arc;
use core::marker::PhantomData;
use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

crate::thread_id!(NamedThread, OtherThread);

/// A thread that picks its own name.
struct Renamed;

impl ThreadId for Renamed {
    fn name() -> &'static str {
        "enc"
    }
}

/// More stack than any OS will map: the spawn itself fails.
const IMPOSSIBLE_STACK: usize = usize::MAX / 2;

/// Records the name of the OS thread it runs on, then closes.
struct RecordName<ThreadIdType>(Arc<Mutex<Option<String>>>, PhantomData<ThreadIdType>);

impl<ThreadIdType> Connection for RecordName<ThreadIdType> {}

impl<ThreadIdType: ThreadId> Workable for RecordName<ThreadIdType> {
    type ThreadId = ThreadIdType;
    fn work(&mut self) -> Result<(), Error> {
        if let Ok(mut name) = self.0.lock() {
            *name = std::thread::current().name().map(Into::into);
        }
        Err(closed!())
    }
}

/// The name of the OS thread a `ThreadIdType` stream runs on.
fn os_name<ThreadIdType: ThreadId>() -> Result<Option<String>, Error> {
    let seen = Arc::new(Mutex::new(None));
    let mut stream = ThreadStream::<ThreadIdType>::new();
    Add::add(&mut stream, Box::new(RecordName(seen.clone(), PhantomData)))?;
    std::thread::scope(|s| assert!(matches!(stream.start(s).join(), Join::Ok)));
    Ok(seen.lock().unwrap().clone())
}

#[test]
fn the_os_thread_is_named_for_its_type() -> Result<(), Error> {
    assert_eq!(os_name::<NamedThread>()?.as_deref(), Some("NamedThread"));
    Ok(())
}

#[test]
fn a_thread_id_can_pick_its_own_name() -> Result<(), Error> {
    assert_eq!(os_name::<Renamed>()?.as_deref(), Some("enc"));
    Ok(())
}

/// The options reach the OS: a stack it can't map fails the spawn, reported by `join`.
#[test]
fn a_refused_spawn_is_reported_by_join() {
    let configured = ThreadStream::<NamedThread>::new()
        .with()
        .stack_size(IMPOSSIBLE_STACK);
    std::thread::scope(|s| {
        let Join::Error(error) = configured.start(s).join() else {
            panic!("expected the refused spawn as Join::Error");
        };
        let never_started = error.downcast_any::<NeverStarted>().unwrap();
        assert_eq!(never_started.thread, "NamedThread");
        assert!(
            never_started.reason.is_some(),
            "join carries the OS's reason"
        );
    });
}

/// Each bundled thread spawns with its own options: the configured one fails alone.
#[test]
fn a_bundle_spawns_each_thread_with_its_own_options() {
    let mut bundle = ThreadBundle::new();
    bundle
        .add(
            ThreadStream::<NamedThread>::new()
                .with()
                .stack_size(IMPOSSIBLE_STACK),
        )
        .add(ThreadStream::<OtherThread>::new());
    std::thread::scope(|s| {
        let joins = bundle.start(s).join();
        let joins: Vec<_> = joins.into_iter().collect();
        assert!(matches!(joins[..], [Join::Error(_), Join::Ok]));
    });
}

/// Records each `Done` it sees: whether it was a [NeverStarted] error.
fn recorder() -> (Arc<Mutex<Vec<bool>>>, impl FnOnce(&Done) + Send + 'static) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    let callback = move |done: &Done| {
        if let Ok(mut seen) = record.lock() {
            seen.push(matches!(done, Done::Error(e) if e.is::<NeverStarted>()));
        }
    };
    (seen, callback)
}

/// A refused spawn is still an exit: `on_done` fires once, as an error.
#[test]
fn on_done_fires_when_the_spawn_is_refused() {
    let (seen, callback) = recorder();
    let mut stream = ThreadStream::<NamedThread>::new();
    stream.on_done(callback);
    let configured = stream.with().stack_size(IMPOSSIBLE_STACK);
    std::thread::scope(|s| drop(configured.start(s)));
    assert_eq!(*seen.lock().unwrap(), vec![true]);
}

/// Same for a poll thread.
#[test]
fn a_poll_threads_on_done_fires_when_the_spawn_is_refused() {
    let (seen, callback) = recorder();
    let mut thread = poll::Thread::<NamedThread>::new();
    thread.on_done(callback);
    let configured = thread.with().stack_size(IMPOSSIBLE_STACK);
    std::thread::scope(|s| drop(configured.start(s)));
    assert_eq!(*seen.lock().unwrap(), vec![true]);
}

/// A bundle hears of a thread that never started through `on_first_error`.
#[test]
fn on_first_error_fires_when_a_bundled_spawn_is_refused() {
    let errors = Arc::new(Mutex::new(0));
    let count = errors.clone();
    let mut bundle = ThreadBundle::new();
    bundle
        .add(
            ThreadStream::<NamedThread>::new()
                .with()
                .stack_size(IMPOSSIBLE_STACK),
        )
        .on_first_error(move |_| {
            if let Ok(mut count) = count.lock() {
                *count += 1;
            }
        });
    std::thread::scope(|s| drop(bundle.start(s).join()));
    assert_eq!(*errors.lock().unwrap(), 1);
}

/// A generic thread type is named without its arguments, not by the last argument's segment.
#[test]
fn a_generic_thread_id_is_named_without_its_arguments() {
    struct Per<Device>(PhantomData<Device>);
    impl<Device: Send + Sync + 'static> ThreadId for Per<Device> {}
    assert_eq!(<Per<NamedThread> as ThreadId>::name(), "Per");
}

/// Flags its own drop: `on_done` promises the thread's work is gone, edges closed, by the
/// time it runs.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Connection for DropFlag {}

impl Workable for DropFlag {
    type ThreadId = NamedThread;
    fn work(&mut self) -> Result<(), Error> {
        Err(closed!())
    }
}

impl GraphBuilder<'static, NamedThread> for DropFlag {
    fn build(
        self: Box<Self>,
        allocator: ThreadLocalWakerAllocator<NamedThread>,
    ) -> Result<Graph<'static, NamedThread>, Error> {
        Ok(Graph::empty(allocator))
    }
}

/// An `on_done` that records whether the work had dropped when it ran.
fn saw_dropped(flag: &Arc<AtomicBool>) -> (Arc<AtomicBool>, impl FnOnce(&Done) + Send + 'static) {
    let seen = Arc::new(AtomicBool::new(false));
    let (dropped, record) = (flag.clone(), seen.clone());
    let callback = move |_: &Done| record.store(dropped.load(Ordering::SeqCst), Ordering::SeqCst);
    (seen, callback)
}

/// A refused spawn drops the workables, closing their edges, before `on_done` runs.
#[test]
fn workables_drop_before_on_done_when_the_spawn_is_refused() -> Result<(), Error> {
    let dropped = Arc::new(AtomicBool::new(false));
    let (seen, callback) = saw_dropped(&dropped);
    let mut stream = ThreadStream::<NamedThread>::new();
    Add::add(&mut stream, Box::new(DropFlag(dropped)))?;
    stream.on_done(callback);
    let configured = stream.with().stack_size(IMPOSSIBLE_STACK);
    std::thread::scope(|s| drop(configured.start(s)));
    assert!(
        seen.load(Ordering::SeqCst),
        "on_done ran with the workables alive"
    );
    Ok(())
}

/// Same for a poll thread's builders, which own its edges until built.
#[test]
fn builders_drop_before_on_done_when_the_spawn_is_refused() {
    let dropped = Arc::new(AtomicBool::new(false));
    let (seen, callback) = saw_dropped(&dropped);
    let mut thread = poll::Thread::<NamedThread>::new();
    thread.add(DropFlag(dropped));
    thread.on_done(callback);
    let configured = thread.with().stack_size(IMPOSSIBLE_STACK);
    std::thread::scope(|s| drop(configured.start(s)));
    assert!(
        seen.load(Ordering::SeqCst),
        "on_done ran with the builders alive"
    );
}
