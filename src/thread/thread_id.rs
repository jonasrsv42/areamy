use std::fmt::Debug;

/// [`ThreadId`]
pub trait ThreadId: Debug + Send + Sync + Clone {}

#[derive(Debug, Clone)]
pub struct DefaultThread {}

impl ThreadId for DefaultThread {}
