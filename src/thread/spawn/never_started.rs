//! [NeverStarted]: the error of a thread the OS refused to spawn.

use crate::any_err;
use crate::error::{AnyErr, Error};

/// A thread the OS refused to spawn: match it with `error.is::<NeverStarted>()`. `on_done`
/// callbacks get it without the OS's reason, because they fire as the refused spawn drops them;
/// `join` returns it with the reason, as `Join::Error`.
#[derive(Debug)]
#[non_exhaustive]
pub struct NeverStarted {
    /// The thread's [ThreadId::name](crate::ThreadId::name).
    pub thread: &'static str,
    /// Why the OS refused; only in `join`'s error.
    pub reason: Option<std::io::Error>,
}

impl core::fmt::Display for NeverStarted {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "thread {} never started", self.thread)?;
        match &self.reason {
            Some(reason) => write!(f, ": {reason}"),
            None => write!(f, ": the OS refused to spawn it"),
        }
    }
}

impl core::error::Error for NeverStarted {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        self.reason
            .as_ref()
            .map(|reason| reason as &(dyn core::error::Error + 'static))
    }
}

impl NeverStarted {
    /// As an areamy [Error].
    pub(crate) fn into_error(self) -> Error {
        any_err!(Box::new(self) as Box<dyn AnyErr>)
    }
}
