//! [Composable] trait for data accumulation during graph traversal.

use crate::error::Error;
/// [`Composable`] is a trait that allows an object to compose an instance of `Self` with
/// type `With` to produce type `To`.
///
/// This is used in an Aremy graph for nodes to attach information to
/// and existing signal or for the composition to yield Self x T => T
///
/// When a computation adds to its input we get
/// 1. Self x From => To
///
/// An example could be if we're passing a multityped list
/// through our graph that aggregates node outputs.
/// This is useful for synchronizing information.
///
/// E.g. what audio frame lead to what text transcritpion.
///
/// We could also get
///
/// 2. Self x From => Self
///
/// In a situation where Self is a struct that
/// can aggregate across many nodes.
///
/// When a computation transforms the input we typically get
/// 3. Self X From => From
///
/// By having types implement this trait we punt the issue
/// of deciding if a computation is a tranformation or
/// addition to the type level. Hence allowing computation nodes
/// to be agnostic to variations in information aggregation.
///
/// Typically primitive types will always be transforms
/// but a user could create a composite type that will
/// aggregate information in some subgraph.
///
/// A compose operation consumes the 'self' type so that
/// it can move fields into a new type without cloning.
pub trait Composable<With> {
    /// [Composable::To] is the unique type produced by Self with With
    type To;

    // Create the instance of the `Self::To`, consuming self allowing for
    // move semantics.
    fn compose(self, argument: With) -> Result<Self::To, Error>;
}

/// [`Decomposable`] is the inverse of [`Composable`]. It splits `Self` into
/// an extracted [`Item`](Decomposable::Item) and a [`Remainder`](Decomposable::Remainder).
///
/// Both halves are fully owned — no partial or invalid states.
///
/// This is useful when a node needs to extract data from a message
/// (e.g. audio samples) for processing, and then compose the remainder
/// with the result.
pub trait Decomposable {
    /// The item extracted from Self.
    type Item;

    /// What remains after extraction.
    type Remainder;

    /// Split self into the extracted item and the remainder.
    fn decompose(self) -> Result<(Self::Item, Self::Remainder), Error>;
}
