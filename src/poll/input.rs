//! Input edge bundles for poll nodes.
//!
//! A poll node's input shape depends on where the data comes from:
//!
//! - [`sync`]: cross-thread input from work or poll producers, over a
//!   [`waking`](crate::edge::waking) edge, plus an [`Input`](sync::Input)
//!   bundle that pairs the receiver with the node it wakes, reserved at mint.
//!
//! - `async`: same-thread input from one or more async parent
//!   nodes. `async::Input` bundles the parent configs;
//!   data flows at runtime through same-thread [`edge::poll`](super::edge::poll)
//!   edges built from those parents.

pub mod r#async;
mod ready;
pub mod sync;

pub(crate) use ready::Ready;
