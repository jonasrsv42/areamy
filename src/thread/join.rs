//! Outcome of joining a thread spawned by [`ThreadStream`] or
//! [`poll::Thread`](crate::thread::poll::stream::Thread).
//!
//! [`Join`] distinguishes clean exit, work-loop error, and OS-thread
//! panic at the type level so callers don't have to pattern-match on
//! `Result<Option<Error>, Error>`.
//!
//! [`BundleJoin`] wraps the per-thread `Vec<Join>` returned by a
//! bundle and exposes [`errors`](BundleJoin::errors) for the common
//! "did anything go wrong" case.

use crate::error::Error;

/// Outcome of joining a single thread.
#[derive(Debug)]
pub enum Join {
    /// Thread exited cleanly — work loop returned `Ok(())` (typically
    /// because all nodes closed naturally).
    Ok,
    /// Work loop returned `Err`. Carries the error.
    Error(Error),
    /// OS thread panicked. Carries the panic payload wrapped via
    /// `fatal!("Thread panicked: {:?}", payload)`.
    Panic(Error),
}

/// Outcome of joining a [`ThreadBundle`](crate::thread::ThreadBundle).
///
/// Holds one [`Join`] per registered thread, in registration order.
/// Iterate directly for full per-thread detail, or call
/// [`errors`](Self::errors) to flatten both error and panic variants
/// into a `Vec<Error>`.
#[derive(Debug)]
pub struct BundleJoin {
    results: Vec<Join>,
}

impl BundleJoin {
    pub fn new(results: Vec<Join>) -> Self {
        Self { results }
    }

    /// All errors — both [`Join::Error`] and [`Join::Panic`] flatten
    /// into the returned `Vec<Error>`. Consumes the bundle result; if
    /// you also need per-thread detail, iterate first.
    pub fn errors(self) -> Vec<Error> {
        self.results
            .into_iter()
            .filter_map(|j| match j {
                Join::Error(e) | Join::Panic(e) => Some(e),
                Join::Ok => None,
            })
            .collect()
    }
}

impl IntoIterator for BundleJoin {
    type Item = Join;
    type IntoIter = std::vec::IntoIter<Join>;

    fn into_iter(self) -> Self::IntoIter {
        self.results.into_iter()
    }
}

/// Slice-like access to the per-thread results — `len`, `is_empty`,
/// `iter`, indexing, `for j in &joins`, etc.
impl std::ops::Deref for BundleJoin {
    type Target = [Join];

    fn deref(&self) -> &[Join] {
        &self.results
    }
}
