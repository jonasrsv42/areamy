//! Assign a thread to a subgraph! [ThreadStream] is a leaf Node of a [std::thread::Thread] that will [crate::Workable::work] all parent nodes.
use crate::error::Error;
use crate::{ThreadId, Workable, fatal, graph::Add};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{JoinHandle, spawn};

/// [`ThreadControl`] is shared mutable state between the main thread and the running thread
/// in this thread stream.
struct ThreadControl<ThreadIdType: ThreadId> {
    /// Should stop is used to indicate from the owning thread that the
    /// node thread should stop. The owning thread will set it to true and then
    /// try to join with it.
    should_stop: Arc<AtomicBool>,

    /// A [JoinHandle] to the node thread. It will return all the [Workable] nodes
    /// on a successful join, handing ownership back to the constructing [std::thread::Thread].
    thread: Option<JoinHandle<Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>>>,
}

/// [`ThreadStream`] is usually as leaf node in a graph that is used to schedule as subgraph
/// onto a dedicated thread. It will [Workable::work] its parents until it is stopped.
pub struct ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId,
{
    workables: Option<Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>>,
    thread_control: ThreadControl<ThreadIdType>,
}

/// [run] runs the dedicated thread and [Workable::work] all the [Workable]s until it is stopped.
fn run<ThreadIdType: ThreadId>(
    mut workables: Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>,
    should_stop: Arc<AtomicBool>,
) -> Vec<Box<dyn Workable<ThreadId = ThreadIdType>>> {
    loop {
        for workable in workables.iter_mut() {
            match workable.work() {
                Ok(()) => (),
                Err(error) => {
                    // We panic the thread on a node error.
                    panic!("{}", error)
                }
            }
        }

        if should_stop.load(Ordering::Relaxed) {
            break;
        }
    }

    return workables;
}

impl<ThreadIdType> ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    pub fn new() -> Box<Self> {
        Box::new(Self {
            workables: Some(Vec::new()),
            thread_control: ThreadControl {
                should_stop: Arc::new(AtomicBool::new(false)),
                thread: None,
            },
        })
    }

    /// Start [Workable::work] on all parents in a dedicated thread. This will
    /// transfer ownership of all [Workable] into the new thread.
    pub fn start(&mut self) -> Result<(), Error> {
        let should_stop = self.thread_control.should_stop.clone();

        match self.workables.take() {
            Some(workables) => {
                Ok(self.thread_control.thread = Some(spawn(move || run(workables, should_stop))))
            }
            None => fatal!("Cannot `start` when workable is None").into(),
        }
    }

    /// Stop [Workable::work] on all parents, join the dedicated thread and transfer
    /// ownership of [Workable] back.
    pub fn stop(&mut self) -> Result<(), Error> {
        let should_stop = self.thread_control.should_stop.clone();
        let handle = self.thread_control.thread.take();

        match handle {
            Some(thread) => {
                should_stop.store(true, Ordering::Relaxed);
                self.workables = Some(thread.join().map_err(|_e| fatal!("JoinHandle error"))?);

                Ok(())
            }
            None => fatal!("Cannot stop unstarted thread").into(),
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
            None => fatal!("Cannot push into active `ThreadStream`.").into(),
        }
    }
}
