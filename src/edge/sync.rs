//! [`Sender`] / [`Receiver`] pair backed by a blocking mutex + condvar
//! queue with refcounted close-on-drop semantics.
//!
//! [`Receiver::new`] creates a single-consumer endpoint with no senders
//! attached. Mint producers with [`Receiver::sender`]; each call
//! increments the producer refcount.
//!
//! When the last [`Sender`] is dropped, the edge auto-closes and any
//! blocked [`Receiver`] read is woken with
//! [`crate::error::ErrorKind::Closed`]. When the [`Receiver`] is
//! dropped, further [`Sender::push_back`] calls return `Closed`.
//!
//! [`Receiver::bounded`] caps how many messages the edge holds. When it is
//! full, [`Sender::push_back`] blocks until the receiver pops or the edge
//! closes, and [`Sender::try_push_back`] hands the message back.
//! Freed slots are not handed out in order: a producer that isn't parked
//! can take one ahead of a parked one, so a busy producer can starve others.

mod receiver;
mod sender;
mod shared;
#[cfg(test)]
pub(crate) mod tests;

pub use receiver::Receiver;
pub use sender::Sender;
pub use shared::State;
