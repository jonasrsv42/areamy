//! Input edge bundles for poll nodes.
//!
//! A poll node's input shape depends on where the data comes from:
//!
//! - [`sync`]: cross-thread input from a sync producer. Thread-safe
//!   `Sender`/`Receiver` with refcounted close-on-drop, plus an
//!   [`Input`](sync::Input) bundle that pairs the receiver with its
//!   pre-allocated waker slot.
//!
//! - [`r#async`]: same-thread input from one or more async parent
//!   nodes. [`Input`](r#async::Input) bundles the parent configs;
//!   data flows at runtime through [`PollEdge`](super::edge::PollEdge)s
//!   built from those parents.

pub mod r#async;
pub mod sync;
