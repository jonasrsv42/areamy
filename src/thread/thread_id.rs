//! [std::thread::Thread] markers.
use std::fmt::Debug;

/// [`ThreadId`] is used to mark a graph node with what group of threads are allowed to schedule it.
pub trait ThreadId: Send + Sync + 'static {}

/// [`DefaultThread`] is the default thread group.
#[derive(Debug)]
pub struct DefaultThread {}

/// [DefaultThread] is a [ThreadId]
impl ThreadId for DefaultThread {}

/// Declares [ThreadId] marker types: `thread_id!(pub EncoderThread, DecoderThread);`
#[macro_export]
macro_rules! thread_id {
    ($($vis:vis $name:ident),+ $(,)?) => {
        $(
            #[derive(Debug, Clone)]
            $vis struct $name;
            impl $crate::ThreadId for $name {}
        )+
    };
}
