//! Input edge bundles for poll nodes.
//!
//! A poll node's input shape depends on where the data comes from:
//!
//! - [`sync`]: cross-thread input from work or poll producers, over a
//!   [`waking`](crate::edge::waking) edge, plus an [`Input`](sync::Input)
//!   bundle that pairs the receiver with its pre-allocated waker slot.
//!
//! - `async`: same-thread input from one or more async parent
//!   nodes. `async::Input` bundles the parent configs;
//!   data flows at runtime through [`PollEdge`](super::edge::PollEdge)s
//!   built from those parents.

pub mod r#async;
pub mod sync;
