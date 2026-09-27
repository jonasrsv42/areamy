use crate::edge::policy::{PolicyEdge, SignalPolicy};
use crate::error::Error;
use crate::graph::marker::Multiplicity;
use crate::graph::{Add, Get, Sink};
use crate::signal::Origin;

/// [`make_push`] creates a [crate::graph::Pushable::push] connection between two nodes.
///
/// A [crate::graph::Pushable::push] is a connection where data flows from parent to child.
///
/// The parent [crate::graph::Pushable::push]es Message data to the child
///
/// * `parent` - A node that we can [Add] a [Sink] too. The parent will [crate::graph::Pushable::push] data into
///   it when the parent is scheduled.
///
/// * `child` - A node that we [Get] the [Sink] from. It will recieve the data when the parent
///   is scheduled.
///
/// The parent is &mut because we mutate it by adding a `Sink` edge to it. The child
/// does not need to be mut so we take an implementation reference to it to avoid
/// moving it.
///
/// This connection uses [SignalPolicy::FollowData] by default, which only forwards signals
/// when they follow data messages. This is a safety measure for cycles in the graph,
/// as using [SignalPolicy::Forward] in back-edges can cause infinite signal propagation loops.
pub fn make_push<
    'params,
    GetMultiplicity: Multiplicity,
    AddMultiplicity: Multiplicity,
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
>(
    parent: &mut impl Add<
        dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
        AddMultiplicity,
    >,
    child: &impl Get<
        dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
        GetMultiplicity,
    >,
) -> Result<(), Error> {
    let pushable = child.get()?;

    // Apply FollowData policy which only forwards signals after data
    // This prevents infinite signal propagation in cyclic graphs
    let policy_edge = Box::new(PolicyEdge::new(pushable, SignalPolicy::FollowData));

    Add::add(parent, policy_edge)?;

    Ok(())
}
