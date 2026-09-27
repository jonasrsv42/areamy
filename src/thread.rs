//! Thread lifecycle management with compile-time state guarantees.
//!
//! Shared runtime glue. The runtimes themselves live in their paradigm:
//! [`crate::work::ThreadStream`] (work) and [`crate::poll::Thread`] (poll).
//! - [`ThreadBundle`] / [`ThreadBundleHandle`]: collections of threads of either kind
//! - [`ThreadId`]: thread identity carried by every node
//!
//! State transitions are enforced at compile time — you cannot start a
//! running thread or join an idle thread.
//!
//! # Example
//!
//! ```ignore
//! thread_id!(MyThread);
//!
//! let thread = ThreadStream::<MyThread>::of(root);
//! std::thread::scope(|s| {
//!     let handle = thread.start(s);   // ThreadStream -> ThreadStreamHandle
//!     // Thread is now running...
//!     match handle.join() {
//!         Join::Ok => { /* clean exit */ }
//!         Join::Error(e) => { /* work loop error */ }
//!         Join::Panic(e) => { /* OS thread panicked */ }
//!     }
//! });
//! ```

mod bundle;
pub(crate) mod callback;
pub(crate) mod done;
pub(crate) mod join;
mod thread_id;
mod type_erase;

pub use bundle::{ThreadBundle, ThreadBundleHandle};
pub use done::{Done, Failure};
pub use join::{BundleJoin, Join};
pub use thread_id::{DefaultThread, ThreadId};
