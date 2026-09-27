//! Thread lifecycle management with compile-time state guarantees.
//!
//! This module provides typestate structs for thread lifecycle:
//! - [`ThreadStream`]: idle thread, can have workables added and be started
//! - [`ThreadStreamHandle`]: running thread, can only be joined
//! - [`ThreadBundle`] / [`ThreadBundleHandle`]: collections of threads
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
mod callback;
mod done;
mod join;
pub mod poll;
mod stream;
mod thread_id;
mod type_erase;

pub use bundle::{ThreadBundle, ThreadBundleHandle};
pub use done::{Done, Failure};
pub use join::{BundleJoin, Join};
pub use stream::{ThreadStream, ThreadStreamHandle};
pub use thread_id::{DefaultThread, ThreadId};
