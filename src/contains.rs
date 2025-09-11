//! [Contains] trait for data access during graph traversal.
//!

use crate::error::Error;
/// [`Contains`] is a trait that allows `Self` to declare that it contains
/// something. It is useful for areamy nodes to restrict inputs/outputs
/// to contain certain data.
///
/// Trait to mark that [self] contains `Item`.
pub trait Contains<Item> {
    // Get a reference to Item
    fn get<'a>(&'a self) -> Result<&'a Item, Error>;
}
