use crate::edge::policy::{Policied, SignalPolicy};
use crate::error::Error;
use crate::graph::marker::Multiplicity;
use crate::graph::{Add, Get, Outputs};
use crate::signal::Origin;
use crate::work::Sink;
use std::marker::PhantomData;

/// [`Push`] is a data-only connection. See [Push::connect].
///
/// The data type is an optional annotation (`Push::<T>::connect`); the signal type is
/// always inferred. Name a side with [crate::graph::At::at].
///
/// ```ignore
/// Push::connect(&mut parent, &child)?;
/// Push::<Frame>::connect(&mut parent, &biunion.at::<Left>())?;
/// ```
pub struct Push<DataType>(PhantomData<DataType>);

impl<DataType: Send + Sync + 'static> Push<DataType> {
    /// [`Push::connect`] creates a [crate::graph::Pushable::push] connection between two nodes.
    ///
    /// A [crate::graph::Pushable::push] is a connection where data flows from parent to child.
    ///
    /// The parent [crate::graph::Pushable::push]es Message data to the child
    ///
    /// * `parent` - A node whose [Outputs] names the sink it takes; we [Add] that sink to it.
    ///   The parent pushes data into it when the parent is scheduled.
    ///
    /// * `child` - A node that we [Get] the parent's sink from. It will recieve the data when
    ///   the parent is scheduled.
    ///
    /// The parent is &mut because we mutate it by adding a `Sink` edge to it. The child
    /// does not need to be mut so we take an implementation reference to it to avoid
    /// moving it.
    ///
    /// This connection uses [SignalPolicy::FollowData] by default, which only forwards signals
    /// when they follow data messages. This is a safety measure for cycles in the graph,
    /// as using [SignalPolicy::Forward] in back-edges can cause infinite signal propagation loops.
    pub fn connect<ParentType, AddMultiplicity, GetMultiplicity>(
        parent: &mut ParentType,
        child: &impl Get<ParentType::Sink, GetMultiplicity>,
    ) -> Result<(), Error>
    where
        ParentType: Outputs<AddMultiplicity>,
        ParentType::Sink: Policied<DataType = DataType>,
        AddMultiplicity: Multiplicity,
        GetMultiplicity: Multiplicity,
    {
        let sink = Policied::with_policy(child.get()?, SignalPolicy::FollowData);
        Add::add(parent, sink)
    }

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
