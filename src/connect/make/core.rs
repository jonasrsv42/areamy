use crate::error::Error;
use crate::{AddPushable, AddWorkable, Connection, GetPushable, GetWorkable, Pushable, Workable};

/// A bidi is a both-way connection between streaming nodes. The ChildType
/// will lend its thread to the parent by working it, and the parent will
/// push data into the childs pushable when available, then return the thread.
///
/// bidi can only connect nodes where the parent has a single output
/// and the child a single input. For more complicated nodes
/// such as connecting to a `bifurcation` output or
/// `biunion` input one should use  `make_push` and `make_work` directly.
pub fn make_bidi<
    ParentType,                       // Generic over parent node
    ChildType,                        // Generic over child node
    ChildConnectionType: Connection,  // Generic over type of child connection
    ParentConnectionType: Connection, // Generic over type of parent connection
>(
    parent: &mut ParentType,
    child: &mut ChildType,
) -> Result<(), Error>
where
    ChildType: 'static
        + AddWorkable<ChildConnectionType, ThreadId = <ParentType::Workable as Workable>::ThreadId>
        + GetPushable<ChildConnectionType>,
    ParentType: 'static
        + AddPushable<ParentConnectionType, Message = <ChildType::Pushable as Pushable>::Message>
        + GetWorkable,
{
    make_push(parent, child)?;
    make_work(parent, child)?;

    Ok(())
}

/// A push is a single connection between objects that lets the parent push
/// into the child upon doing work.
pub fn make_push<ParentType, ChildType, GetConnection: Connection, AddConnection: Connection>(
    parent: &mut ParentType,
    child: &ChildType,
) -> Result<(), Error>
where
    ChildType: GetPushable<GetConnection>,
    ParentType: AddPushable<AddConnection, Message = <ChildType::Pushable as Pushable>::Message>,
{
    let pushable = child.get()?;
    AddPushable::add(parent, pushable)?;

    Ok(())
}

/// A work is a single connection between objects that let a child
/// tell a parent to do work.
pub fn make_work<ParentType, ChildType, AddConnection: Connection>(
    parent: &ParentType,
    child: &mut ChildType,
) -> Result<(), Error>
where
    ChildType: AddWorkable<AddConnection, ThreadId = <ParentType::Workable as Workable>::ThreadId>,
    ParentType: GetWorkable + 'static,
{
    let workable = parent.get()?;
    AddWorkable::add(child, workable)?;

    Ok(())
}
