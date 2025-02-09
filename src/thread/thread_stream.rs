use crate::error::Error;
use crate::{fatal, graph::Add, ThreadId, Workable};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{spawn, JoinHandle};

struct ThreadControl {
    should_stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

pub struct ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId,
{
    workables: Arc<Mutex<Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>>>,
    thread_control: ThreadControl,
}

fn run<ThreadIdType: ThreadId>(
    workables: Arc<Mutex<Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>>>,
    should_stop: Arc<AtomicBool>,
) {
    let mut owned_workables = workables
        .lock()
        .expect("Failed to grab ownership of workables in thread");
    loop {
        for workable in owned_workables.iter_mut() {
            let _ = workable.work();
        }

        if should_stop.load(Ordering::Relaxed) {
            break;
        }
    }
}

impl<ThreadIdType> ThreadStream<ThreadIdType>
where
    ThreadIdType: ThreadId + 'static,
{
    pub fn new() -> Box<Self> {
        Box::new(Self {
            workables: Arc::new(Mutex::new(Vec::new())),
            thread_control: ThreadControl {
                should_stop: Arc::new(AtomicBool::new(false)),
                thread: None,
            },
        })
    }

    pub fn start(&mut self) -> Result<(), Error> {
        let workables = self.workables.clone();
        let should_stop = self.thread_control.should_stop.clone();

        let thread = spawn(move || run(workables, should_stop));

        self.thread_control.thread = Some(thread);

        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), Error> {
        let should_stop = self.thread_control.should_stop.clone();
        let handle = self.thread_control.thread.take();

        match handle {
            Some(thread) => {
                should_stop.store(true, Ordering::Relaxed);
                thread.join().map_err(|_e| fatal!("JoinHandle error"))?;

                Ok(())
            }
            None => fatal!("Cannot stop unstarted thread").into(),
        }
    }
}

impl<ThreadIdType: ThreadId> Add<dyn Workable<ThreadId = ThreadIdType>>
    for ThreadStream<ThreadIdType>
{
    fn add(&mut self, workable: Box<dyn Workable<ThreadId = ThreadIdType>>) -> Result<(), Error> {
        let mut workables = self.workables.lock().map_err(|e| fatal!(e))?;
        workables.push(workable);
        Ok(())
    }
}
