//! Thread bundle for managing multiple heterogeneous threads as a unit.
//!
//! - [`core`]: [`ThreadBundle`] + [`ThreadBundleHandle`] — the user-facing API.
//! - [`first_error`]: at-most-once callback injection for the
//!   [`on_first_error`](ThreadBundle::on_first_error) helper.

mod core;
mod first_error;

#[cfg(test)]
mod fixtures;

pub use core::{ThreadBundle, ThreadBundleHandle};
