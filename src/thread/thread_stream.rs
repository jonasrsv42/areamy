//! Assign a thread to a subgraph! [ThreadStream] is a leaf Node of a [std::thread::Thread] that will [crate::Workable::work] all parent nodes.
use crate::ThreadLike;
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

/// [`ThreadStream`] is usually a leaf node in a graph that is used to schedule a subgraph
/// onto a dedicated thread. It will [Workable::work] its parents until a source signals
/// [ErrorKind::Exhausted] or an error occurs.
///
/// The thread can be restarted after joining - sources can be swapped or modified
/// and [ThreadStream::start] called again.
pub struct ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    workables: Option<Workables<ThreadIdType>>,
    thread: Option<JoinHandle<Result<Workables<ThreadIdType>, JoinError<ThreadIdType>>>>,
}

/// Run loop that works all workables until one returns an error.
/// Returns Ok(()) on [ErrorKind::Exhausted], propagates other errors.
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

/// [run] runs the dedicated thread and [Workable::work] all the [Workable]s until
/// a source is exhausted or an error occurs.
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

impl<ThreadIdType> ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    pub fn new() -> Box<Self> {
        Box::new(Self {
            workables: Some(Vec::new()),
            thread: None,
        })
    }
}

impl<ThreadIdType: ThreadId> ThreadLike for ThreadStream<ThreadIdType> {
    /// Start [Workable::work] on all parents in a dedicated thread. This will
    /// transfer ownership of all [Workable] into the new thread.
    ///
    /// The thread will run until a workable returns [ErrorKind::Exhausted]
    /// (clean exit) or another error (failure). Call [ThreadStream::join]
    /// to wait for completion and retrieve workables.
    ///
    /// The stream is restartable - after [ThreadStream::join] returns,
    /// sources can be modified and [ThreadStream::start] called again.
    fn start(&mut self) -> Result<(), Error> {
        match self.workables.take() {
            Some(workables) => {
                self.thread = Some(spawn(move || run(workables)));
                Ok(())
            }
            None => fatal!("Cannot `start` when workables already taken (thread running?)").into(),
        }
    }

    /// Join the dedicated thread, waiting for it to complete.
    /// Returns ownership of [Workable]s back to this struct.
    ///
    /// Returns `Ok(())` if the thread exited cleanly (via [ErrorKind::Exhausted]).
    /// Returns `Err(error)` if a workable returned an error.
    fn join(&mut self) -> Result<(), Error> {
        match self.thread.take() {
            Some(handle) => {
                match handle.join().map_err(|err| {
                    fatal!(
                        "Joining thread {} panicked: {:?}",
                        std::any::type_name::<ThreadIdType>(),
                        err
                    )
                })? {
                    Ok(workables) => {
                        self.workables = Some(workables);
                        Ok(())
                    }
                    Err(join_err) => {
                        self.workables = Some(join_err.workables);
                        Err(join_err.error)
                    }
                }
            }
            None => fatal!("Cannot join: no thread running").into(),
        }
    }
}

/// Add [Workable] nodes to this node to be scheduled by its dedicated thread, when started.
impl<ThreadIdType: ThreadId> Add<dyn Workable<ThreadId = ThreadIdType>>
    for ThreadStream<ThreadIdType>
{
    fn add(&mut self, workable: Box<dyn Workable<ThreadId = ThreadIdType>>) -> Result<(), Error> {
        match self.workables.as_mut() {
            Some(workables) => Ok(workables.push(workable)),
            None => fatal!(
                "Cannot add to active `ThreadStream` for {}.",
                std::any::type_name::<ThreadIdType>()
            )
            .into(),
        }
    }
}

impl<ThreadIdType: ThreadId + 'static> Drop for ThreadStream<ThreadIdType> {
    fn drop(&mut self) {
        if self.thread.is_some() {
            // Best-effort cleanup - thread should have exited via Exhausted
            let _ = self.join();
        }
    }
}
