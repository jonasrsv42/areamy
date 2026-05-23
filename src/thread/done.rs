//! Exit notification passed to [`on_done`] callbacks.
//!
//! [`on_done`]: crate::thread::ThreadStream::on_done
//!
//! Constructed inside the spawn closure right before the thread
//! terminates. This is distinct from [`Join`](crate::thread::Join),
//! which is the value returned by `handle.join()` after the OS thread
//! has been joined.

use crate::error::Error;

/// How a thread is exiting.
#[derive(Debug)]
pub enum Done<'a> {
    /// Work loop returned `Ok(())` — all nodes closed naturally.
    Close,
    /// Work loop returned `Err`. The error is borrowed for the
    /// duration of the callback; clone fields if you need to retain
    /// anything beyond the call.
    Error(&'a Error),
    /// The OS thread panicked. The panic payload is *not* available
    /// here (the spawn closure cannot see its own unwinding payload);
    /// the formatted payload is available later via the matching
    /// [`Join::Panic`](crate::thread::Join::Panic) returned from
    /// `handle.join()`.
    Panic,
}

/// Failure-only subset of [`Done`].
///
/// Passed to [`ThreadBundle::on_first_error`] callbacks — clean
/// exits don't trigger that callback, so the `Close` variant has no
/// place in the type.
///
/// [`ThreadBundle::on_first_error`]: crate::thread::ThreadBundle::on_first_error
#[derive(Debug)]
pub enum Failure<'a> {
    /// Work loop returned `Err`. Error borrowed for the duration of
    /// the callback.
    Error(&'a Error),
    /// The OS thread panicked. As with [`Done::Panic`], the payload
    /// is not available here.
    Panic,
}

impl<'a> Failure<'a> {
    /// Project a [`Done`] into a [`Failure`], returning `None` for
    /// the `Close` variant.
    pub fn from_done(done: &'a Done<'a>) -> Option<Self> {
        match done {
            Done::Close => None,
            Done::Error(e) => Some(Failure::Error(e)),
            Done::Panic => Some(Failure::Panic),
        }
    }
}
