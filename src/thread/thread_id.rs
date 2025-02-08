use std::fmt::Debug;

pub trait ThreadId: Debug + Send + Sync + Clone {}

#[derive(Debug, Clone)]
pub struct DefaultThread {}

impl ThreadId for DefaultThread {}
