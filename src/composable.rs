use crate::error::Error;
use std::sync::Arc;
/// Trait that allows and object to compose an instance of `Self` with
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
pub trait Composable<With, To> {
    // Create the instance of the type
    fn compose(&self, argument: With) -> Result<To, Error>;
}

// Composing with a type on heap yields a type on the heap, this allows
// Routine signatures to be ignorant about wether the input type is on the
// heap or not. However it restricts it such that if input is in heap
// then output is on heap. Hopefully this does not lead to too much pain.
// It seems neat at the time of writing.
impl<With, To, ComposableType: Composable<With, To>> Composable<With, Arc<To>>
    for Arc<ComposableType>
{
    fn compose(&self, argument: With) -> Result<Arc<To>, Error> {
        self.as_ref().compose(argument).map(Arc::new)
    }
}
