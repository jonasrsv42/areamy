use crate::edge::policy::{Policied, SignalPolicy};
use crate::error::Error;
use crate::graph::marker::{Multiplicity, Unary};
use crate::graph::{Add, Get, Outputs};
use crate::signal::Origin;
use crate::work::Sink;
use std::marker::PhantomData;

/// [`Pushes`] is the data-only edge: `up.pushes(&down)?`. `up` keeps running where it is and
/// pushes into `down`'s input.
///
/// Both ends are single-sided; name a side with [crate::At::at_mut] (upstream) or
/// [crate::At::at_ref] (downstream). The data type is an optional annotation:
/// `up.pushes::<Frame>(&down)?`.
///
/// Signals follow data ([SignalPolicy::FollowData]): a push can be a back-edge, where
/// forwarding every signal would loop.
///
/// ```ignore
/// up.pushes(&down)?;
/// router.at_mut::<Right>().pushes(&noise)?;
/// up.pushes::<Frame>(&merge.at_ref::<Left>())?;
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no single output to push from",
    note = "a node with several outputs needs a borrowed side: `node.at_mut::<Left>().pushes(..)`",
    note = "`at` moves the node and only feeds `via`; `pushes` takes `at_mut`"
)]
pub trait Pushes: Outputs<Unary> {
    fn pushes<DataType>(&mut self, down: &impl Get<Self::Sink, Unary>) -> Result<(), Error>
    where
        DataType: Send + Sync + 'static,
        Self::Sink: Policied<DataType = DataType>;
}

impl<NodeType: Outputs<Unary>> Pushes for NodeType {
    fn pushes<DataType>(&mut self, down: &impl Get<Self::Sink, Unary>) -> Result<(), Error>
    where
        DataType: Send + Sync + 'static,
        Self::Sink: Policied<DataType = DataType>,
    {
        let sink = Policied::with_policy(down.get()?, SignalPolicy::FollowData);
        Add::add(self, sink)
    }
}

/// [`Push`] pins the data type of an input that stays unconnected. See [Push::open].
pub struct Push<DataType>(PhantomData<DataType>);

impl<DataType: Send + Sync + 'static> Push<DataType> {
    /// Pin the data type of an input that stays unconnected (e.g. fed by a
    /// [crate::work::Writer] elsewhere).
    pub fn open<'params, GetMultiplicity: Multiplicity>(
        _child: &impl Get<
            dyn Sink<DataType = DataType, SignalType = impl Origin + 'static>
                + Send
                + Sync
                + 'params,
            GetMultiplicity,
        >,
    ) {
    }
}
