use crate::error::Error;

/// [`Downstream`] is how a node takes ownership of its upstream: schedule + data. areamy's work
/// nodes implement it in [crate::work], poll builders next to their builders.
///
/// It is the extension point for custom work nodes. Take the upstream through
/// [crate::work::Upstream] (a whole node, or a side moved with [crate::At::at]), give it a
/// forwarding edge into this node's input, and own it:
///
/// ```ignore
/// impl<Up, Shape> Downstream<Up, Frame, OwnedWork<Shape, Signal, Thread>> for MyNode
/// where
///     Up: Upstream<'static, Frame, Signal, Thread, Shape>, // MyNode: Get<sink> + Add<Workable>
/// {
///     type Out = Self;
///     fn own(mut self, upstream: Up) -> Result<Self, Error> {
///         let sink = Get::get(&self)?;
///         let upstream = upstream.feed(Box::new(PolicyEdge::new(sink, SignalPolicy::Forward)))?;
///         Add::add(&mut self, upstream)?;
///         Ok(self)
///     }
/// }
/// ```
///
/// Poll builders take an [crate::poll::AsyncParent] instead.
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
    /// What `via` returns: this node, or the resolved builder.
    type Out;
    /// Owns `upstream`: `down.own(up)` is `up.via(down)`.
    fn own(self, upstream: UpstreamType) -> Result<Self::Out, Error>;
}

/// A boxed downstream takes its upstream like the node inside, and stays boxed.
impl<UpstreamType, DataType, OwnedType, DownstreamType>
    Downstream<UpstreamType, DataType, OwnedType> for Box<DownstreamType>
where
    DownstreamType: Downstream<UpstreamType, DataType, OwnedType>,
{
    type Out = Box<DownstreamType::Out>;
    fn own(self, upstream: UpstreamType) -> Result<Self::Out, Error> {
        Ok(Box::new((*self).own(upstream)?))
    }
}
