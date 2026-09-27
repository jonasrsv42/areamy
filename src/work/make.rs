use crate::edge::policy::{PolicyEdge, SignalPolicy};
use crate::error::Error;
use crate::graph::marker::Multiplicity;
use crate::graph::{Add, Get, Sink};
use crate::signal::Origin;
use crate::work::Workable;

/// [`make_bidi`] creates a `bidi` connection between two nodes.
///
/// A `bidi` (bidirectional) is a connection where data flows from parent to child and scheduling
/// from child to parent.
///
/// The child lends its `ThreadIdType` to the parent for it to `work` and then `push` the `MessageType`
/// back into the child.
///
/// * `parent` - A [Workable] that we can [Add] a [Sink] into. The
///   [Workable] will be [Add] to the child so the child can schedule the [Workable::work]. The
///   [Sink] will be [Add]ed as output for the child so it can [crate::graph::Pushable::push] into the parent.
///
/// * `child` - A type that we can [Add] the parent [Workable] into to grab ownership and from which we can [Get] the
///   [Sink] and give to the parent.
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
    'params,
    ParentType,
    ChildMultiplicity: Multiplicity, // Generic over type of child connection
    ParentMultiplicity: Multiplicity, // Generic over type of parent connection
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    ThreadIdType,
>(
    mut parent: Box<ParentType>,
    child: &mut (
             impl Add<dyn Workable<ThreadId = ThreadIdType> + 'params, ChildMultiplicity>
             + Get<
        dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
        ChildMultiplicity,
    >
         ),
) -> Result<(), Error>
where
    ParentType: Add<
            dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params,
            ParentMultiplicity,
        > + Workable<ThreadId = ThreadIdType>
        + 'params,
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

/// [`make_work`] creates a `work` connection between two nodes.
///
/// A `work` connection in a scheduling connection. In this case the child can make the parent
/// work.
///
/// * `parent` - A [Workable]. The parent will be [Add]ed to the child so the
///   child can schedule the work.
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
pub fn make_work<'params, MultiplicityType: Multiplicity, ThreadIdType>(
    parent: Box<impl Workable<ThreadId = ThreadIdType> + 'params>,
    child: &mut impl Add<dyn Workable<ThreadId = ThreadIdType> + 'params, MultiplicityType>,
) -> Result<(), Error>
where
{
    Add::add(child, parent)?;

    Ok(())
}
