//! [std::thread::Thread] markers.
use std::fmt::Debug;

/// [`ThreadId`] is used to mark a graph node with what group of threads are allowed to schedule it.
pub trait ThreadId: Debug + Send + Sync + Clone {}

/// [`DefaultThread`] is the default thread group.
#[derive(Debug, Clone)]
pub struct DefaultThread {}

/// [DefaultThread] is a [ThreadId]
impl ThreadId for DefaultThread {}
