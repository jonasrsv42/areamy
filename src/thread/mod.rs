//! A node with a dedicated [std::thread::Thread] that will [crate::Workable::work] on all parents.

use crate::error::Error;

pub mod thread_id;
pub mod thread_stream;

pub trait Thread {
    /// Start running the work owned by the [Thread]. A thread has to be
    /// joined to the main thread before it can be started again. A double start
    /// will yield an error.
    fn start(&mut self) -> Result<(), Error>;

    /// Join the thread. Join has to follow a start, a double join will yield an error.
    fn join(&mut self) -> Result<(), Error>;
}
