use crate::error::Error;

/// [`Downstream`] is how a node takes ownership of its upstream: schedule + data. Work nodes
/// implement it in [crate::work], poll builders next to their builders.
///
/// `DataType` is a trait parameter so the caller may state it; `OwnedType` carries each impl's
/// otherwise unconstrained parameters.
#[diagnostic::on_unimplemented(
    message = "`{UpstreamType}` cannot be an owning upstream of `{Self}`",
    label = "no owning edge from `{UpstreamType}` into this",
    note = "a node with several sides needs one named: `up.at::<Side>().via(down.at::<Side>())`",
    note = "an owning edge moves its ends: `at` moves a side, `at_mut` / `at_ref` only borrow",
    note = "a work node cannot own a poll builder and a poll node cannot own a work node: bridge with a Sync input (`.input::<Sync>()` + `pushes`)"
)]
pub trait Downstream<UpstreamType, DataType, OwnedType> {
    type Out;
    fn take(self, upstream: UpstreamType) -> Result<Self::Out, Error>;
}
