use crate::error::Error;
use crate::{
    graph::{Add, Get},
    marker::Multiplicity,
    Pushable, Workable,
};

/// `make_bidi` creates a `bidi` connection between two nodes.
///
/// A `bidi` (bidirectional) is a connection where data flows from parent to child and scheduling
/// from child to parent.
///
/// The child lends its `ThreadIdType` to the parent for it to `work` and then `push` the `MessageType`
/// back into the child.
///
/// * `parent` - A `Workable` that we can add a (Pushable) into. The
///     (Workable) will be added to the child so the child can schedule the work. The (Pushable)
///     will be added as output for the child so it can push into the parent. The parent is a
///     Box<...> because ownership of it will be transferred into the child. Hence calling
///     `make_bidi` should be the last thing done for a node. By transferring ownership
///     we for example help prevent construction of graphs with deadlocks.
///
/// * `child` - A type that we can add the parent (Workable) into to grab ownership and from which we can retrieve the
///     (Pushable) and give to the parent.
///
/// After this call, the `child` will own the parent. The child can keep being connected into
/// things in the graph, but the `parent` is done.
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

/// `make_push` creates a `push` connection between two nodes.
///
/// A `push` (push connection) is a connection where data flows from parent to child.
///
/// The child parent `push` Message data to the parent
///
/// * `parent` - A node that we can add a `Pushable` too. The parent will push data into 
///     it when the parent is worked on.
///
/// * `child` - A node that we fetch the `Pushable` from. It will recieve the data when the parent
///     is worked on.
///
/// The parent is &mut because we mutate it by adding a `Pushable` edge to it.
pub fn make_push<GetMultiplicity: Multiplicity, AddMultiplicity: Multiplicity, MessageType>(
    parent: &mut impl Add<dyn Pushable<Message = MessageType>, AddMultiplicity>,
    child: &impl Get<dyn Pushable<Message = MessageType>, GetMultiplicity>,
) -> Result<(), Error> {
    let pushable = child.get()?;
    Add::add(parent, pushable)?;

    Ok(())
}

/// TODO...
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
