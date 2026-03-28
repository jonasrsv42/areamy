//! Builders for [super::node::AsyncLine].
//!
//! Each builder corresponds to a linkage role:
//! - [terminal]: No linkage. Sync input, sync output.
//! - [parent]: Linkable<Parent>. Sync input, async output.
//! - [child]: Linkable<Child> + Spawnable. Async input, sync output. Owns parent.
//! - [linked]: Linkable<Parent> + Linkable<Child>. Async in + async out. Owns parent.

pub mod child;
pub mod linked;
pub mod parent;
pub mod terminal;
