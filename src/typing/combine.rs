//! [Combine] trait for many-to-one data aggregation.

use crate::error::Error;

/// [`Combine`] is a trait that allows combining multiple instances of `Self`
/// into a single instance of type `Combined`.
///
/// This is the many:1 counterpart to:
/// - [`Composable`](crate::Composable) which is 1:1 (Self × T → To)
/// - [`Generates`](crate::Generates) which is 1:many (Self → multiple T)
///
/// # Use Cases
///
/// - Blocking: combining N audio frames into a single block
/// - Aggregation: merging multiple partial results
/// - Batching: grouping items for batch processing
///
/// # Efficiency
///
/// The trait receives ownership of all items via `Vec<Self>`, allowing
/// implementations to move data efficiently. For example, when combining
/// `Vec<Array1<T>>` into `Array2<T>`:
///
/// ```ignore
/// fn combine(items: Vec<Self>) -> Result<Self::Combined, Error> {
///     let n_rows = items.len();
///     let n_cols = items[0].0.len();
///     let mut data = Vec::with_capacity(n_rows * n_cols);
///     for item in items {
///         // into_raw_vec moves data when layout permits
///         data.extend(item.0.into_raw_vec());
///     }
///     let block = Array2::from_shape_vec((n_rows, n_cols), data)?;
///     Ok(BlockedEncoding(block))
/// }
/// ```
///
/// Alternatively, use `ndarray::stack` for a cleaner API:
///
/// ```ignore
/// fn combine(items: Vec<Self>) -> Result<Self::Combined, Error> {
///     let views: Vec<_> = items.iter().map(|e| e.0.view()).collect();
///     let block = stack(Axis(0), &views)?;
///     Ok(BlockedEncoding(block))
/// }
/// ```
pub trait Combine {
    /// The type produced by combining multiple instances of Self.
    type Combined;

    /// Combine multiple items into one.
    ///
    /// Receives ownership of all items to allow efficient extraction/moving.
    /// The implementation decides how to aggregate both data and metadata.
    fn combine(items: Vec<Self>) -> Result<Self::Combined, Error>
    where
        Self: Sized;
}
