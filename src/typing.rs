//! Traits for building routines that are generic over their data types.

mod combine;
mod composable;
mod contains;
mod generates;

pub use combine::Combine;
pub use composable::{Composable, Decomposable};
pub use contains::Contains;
pub use generates::Generates;
