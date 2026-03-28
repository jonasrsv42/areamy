use crate::error::Error;
use crate::{
    Closeable, PolicyEdge, SignalPolicy, Workable,
    graph::{Add, Get},
    marker::Multiplicity,
    signal::Origin,
};

/// [`make_bidi`] creates a `bidi` connection between two nodes.
///
/// A `bidi` (bidirectional) is a connection where data flows from parent to child and scheduling
/// from child to parent.
///
/// The child lends its `ThreadIdType` to the parent for it to `work` and then `push` the `MessageType`
/// back into the child.
///
/// * `parent` - A [Workable] that we can [Add] a [Closeable] into. The
///     [Workable] will be [Add] to the child so the child can schedule the [Workable::work]. The
///     [Closeable] will be [Add]ed as output for the child so it can [Closeable::push] into the parent.
///
/// * `child` - A type that we can [Add] the parent [Workable] into to grab ownership and from which we can [Get] the
///     [Closeable] and give to the parent.
///
///
/// The function takes the parent by value as its ownership will be transferred into the child.
/// The child is taken by mutable reference as we will be adding the parent to it.
///
/// After this call, the `child` will own the parent. The child can keep being connected into
/// things in the graph, but the `parent` is done.
///
/// By transferring ownership upon scheduling connection we make it hard to introduce bugs such as
/// scheduling circles, which would be deadlocks.
///
/// This connection uses [SignalPolicy::Forward] for data flow between parent and child,
/// which always forwards signals. Due to our ownership semantics, building cycles of
/// bidi chains should be impossible, so Forward is a safe default here without risk
/// of infinite signal propagation.
pub fn make_bidi<
    ParentType,
    ChildMultiplicity: Multiplicity, // Generic over type of child connection
    ParentMultiplicity: Multiplicity, // Generic over type of parent connection
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType,
>(
    mut parent: Box<ParentType>,
    child: &mut (
             impl Add<dyn Workable<ThreadId = ThreadIdType>, ChildMultiplicity>
             + Get<
        dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
        ChildMultiplicity,
    >
         ),
) -> Result<(), Error>
where
    ParentType: Add<
            dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
            ParentMultiplicity,
        > + Workable<ThreadId = ThreadIdType>
        + 'static,
{
    // We need to get the pushable manually and apply Forward policy
    let pushable = child.get()?;

    // Apply Forward policy to allow all signals to flow from parent to child
    // Due to our ownership semantics, building cycles of bidi chains should be impossible,
    // so Forward is a safe default here without risk of infinite signal propagation
    let policy_edge = Box::new(PolicyEdge::new(pushable, SignalPolicy::Forward));

    // Add the pushable with Forward policy to the parent
    Add::add(parent.as_mut(), policy_edge)?;

    // Continue with the work connection
    make_work(parent, child)?;

    Ok(())
}

/// [`make_push`] creates a [Closeable::push] connection between two nodes.
///
/// A [Closeable::push] is a connection where data flows from parent to child.
///
/// The parent [Closeable::push]es Message data to the child
///
/// * `parent` - A node that we can [Add] a [Closeable] too. The parent will [Closeable::push] data into
///     it when the parent is scheduled.
///
/// * `child` - A node that we [Get] the [Closeable] from. It will recieve the data when the parent
///     is scheduled.
///
/// The parent is &mut because we mutate it by adding a `Closeable` edge to it. The child
/// does not need to be mut so we take an implementation reference to it to avoid
/// moving it.
///
/// This connection uses [SignalPolicy::FollowData] by default, which only forwards signals
/// when they follow data messages. This is a safety measure for cycles in the graph,
/// as using [SignalPolicy::Forward] in back-edges can cause infinite signal propagation loops.
pub fn make_push<
    GetMultiplicity: Multiplicity,
    AddMultiplicity: Multiplicity,
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
>(
    parent: &mut impl Add<
        dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
        AddMultiplicity,
    >,
    child: &impl Get<
        dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
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

/// [`make_work`] creates a `work` connection between two nodes.
///
/// A `work` connection in a scheduling connection. In this case the child can make the parent
/// work.
///
/// * `parent` - A [Workable]. The parent will be [Add]ed to the child so the
///     child can schedule the work.
///
/// * `child` - A type that we can [Add] the parent [Workable] to for scheduling.
///
/// The function takes the parent by value as its ownership will be transferred to the child.
/// The child is taken by mutable reference as we will be adding the parent to it.
///
/// After this call, the `child` will own the parent. The child can keep being connected into
/// things in the graph, but the `parent` is done.
///
/// <div class="info">  
/// By transferring ownership upon scheduling connection we make it hard to introduce bugs such as
/// scheduling circles, which would be deadlocks.
/// </div>
///
pub fn make_work<MultiplicityType: Multiplicity, ThreadIdType>(
    parent: Box<impl Workable<ThreadId = ThreadIdType> + 'static>,
    child: &mut impl Add<dyn Workable<ThreadId = ThreadIdType>, MultiplicityType>,
) -> Result<(), Error>
where
{
    Add::add(child, parent)?;

    Ok(())
}
