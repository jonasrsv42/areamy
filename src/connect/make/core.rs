use crate::error::Error;
use crate::{
    graph::{Add, Get},
    marker::Multiplicity,
    Pushable, Workable,
};

/// [`make_bidi`] creates a `bidi` connection between two nodes.
///
/// A `bidi` (bidirectional) is a connection where data flows from parent to child and scheduling
/// from child to parent.
///
/// The child lends its `ThreadIdType` to the parent for it to `work` and then `push` the `MessageType`
/// back into the child.
///
/// * `parent` - A [Workable] that we can add a (Pushable) into. The
///     [Workable] will be added to the child so the child can schedule the work. The (Pushable)
///     will be added as output for the child so it can push into the parent.
///
/// * `child` - A type that we can add the parent [Workable] into to grab ownership and from which we can retrieve the
///     (Pushable) and give to the parent.
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
pub fn make_bidi<
    ParentType,
    ChildMultiplicity: Multiplicity, // Generic over type of child connection
    ParentMultiplicity: Multiplicity, // Generic over type of parent connection
    MessageType,
    ThreadIdType,
>(
    mut parent: Box<ParentType>,
    child: &mut (impl Add<dyn Workable<ThreadId = ThreadIdType>, ChildMultiplicity>
              + Get<dyn Pushable<Message = MessageType>, ChildMultiplicity>),
) -> Result<(), Error>
where
    ParentType: Add<dyn Pushable<Message = MessageType>, ParentMultiplicity>
        + Workable<ThreadId = ThreadIdType>
        + 'static,
{
    make_push(parent.as_mut(), child)?;
    make_work(parent, child)?;

    Ok(())
}

/// [`make_push`] creates a `push` connection between two nodes.
///
/// A `push` (push connection) is a connection where data flows from parent to child.
///
/// The parent `push`es Message data to the child
///
/// * `parent` - A node that we can add a [Pushable] too. The parent will push data into
///     it when the parent is scheduled.
///
/// * `child` - A node that we fetch the `Pushable` from. It will recieve the data when the parent
///     is scheduled.
///
/// The parent is &mut because we mutate it by adding a `Pushable` edge to it. The child
/// does not need to be mut so we take an implementation reference to it to avoid
/// moving it.
pub fn make_push<GetMultiplicity: Multiplicity, AddMultiplicity: Multiplicity, MessageType>(
    parent: &mut impl Add<dyn Pushable<Message = MessageType>, AddMultiplicity>,
    child: &impl Get<dyn Pushable<Message = MessageType>, GetMultiplicity>,
) -> Result<(), Error> {
    let pushable = child.get()?;
    Add::add(parent, pushable)?;

    Ok(())
}

/// [`make_work`] creates a `work` connection between two nodes.
///
/// A `work` connection in a scheduling connection. In this case the child can make the parent
/// work.
///
/// * `parent` - A [Workable]. The parent will be added to the child so the 
///     child can schedule the work. 
///
/// * `child` - A type that we can add the parent [Workable] to for scheduling.
///
/// The function takes the parent by value as its ownership will be transferred to the child.
/// The child is taken by mutable reference as we will be adding the parent to it.
///
/// After this call, the `child` will own the parent. The child can keep being connected into
/// things in the graph, but the `parent` is done.
///
///
/// <div class="info">  
/// By transferring ownership upon scheduling connection we make it hard to introduce bugs such as
/// scheduling circles, which would be deadlocks.
/// </div>
/// 
pub fn make_work<ParentType, ChildMultiplicity: Multiplicity, ThreadIdType>(
    parent: Box<ParentType>,
    child: &mut impl Add<dyn Workable<ThreadId = ThreadIdType>, ChildMultiplicity>,
) -> Result<(), Error>
where
    ParentType: Workable<ThreadId = ThreadIdType> + 'static,
{
    Add::add(child, parent)?;

    Ok(())
}
