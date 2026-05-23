//! Cross-thread sync edges with refcounted close-on-drop semantics.
//!
//! [`Receiver::new`] creates a single-consumer endpoint with no senders
//! attached. Mint producers with [`Receiver::sender`]; each call
//! increments the producer refcount.
//!
//! When the last [`Sender`] is dropped, the edge auto-closes and any
//! blocked [`Receiver`] read is woken with
//! [`crate::error::ErrorKind::Closed`]. When the [`Receiver`] is
//! dropped, further [`Sender::push_back`] calls return `Closed`.

pub mod edge;

pub use edge::{Receiver, Sender};
