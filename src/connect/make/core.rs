use crate::error::Error;
use crate::{
    graph::{Add, Get},
    marker::Multiplicity,
    Pushable, Workable,
};

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
    ChildMultiplicity: Multiplicity,  // Generic over type of child connection
    ParentMultiplicity: Multiplicity, // Generic over type of parent connection
    MessageType,
    ThreadIdType,
>(
    parent: &mut ParentType,
    child: &mut ChildType,
) -> Result<(), Error>
where
    ChildType: 'static
        + Add<dyn Workable<ThreadId = ThreadIdType>, ChildMultiplicity>
        + Get<dyn Pushable<Message = MessageType>, ChildMultiplicity>,
    ParentType: 'static
        + Add<dyn Pushable<Message = MessageType>, ParentMultiplicity>
        + Get<dyn Workable<ThreadId = ThreadIdType>, ParentMultiplicity>,
{
    make_push(parent, child)?;
    make_work(parent, child)?;

    Ok(())
}

/// A push is a single connection between objects that lets the parent push
/// into the child upon doing work.
pub fn make_push<GetMultiplicity: Multiplicity, AddMultiplicity: Multiplicity, MessageType>(
    parent: &mut impl Add<dyn Pushable<Message = MessageType>, AddMultiplicity>,
    child: &impl Get<dyn Pushable<Message = MessageType>, GetMultiplicity>,
) -> Result<(), Error> {
    let pushable = child.get()?;
    Add::add(parent, pushable)?;

    Ok(())
}

/// A work is a single connection between objects that let a child
/// tell a parent to do work.
pub fn make_work<
    ChildMultiplicity: Multiplicity,
    ParentMultiplicity: Multiplicity,
    ThreadIdType,
>(
    parent: &impl Get<dyn Workable<ThreadId = ThreadIdType>, ParentMultiplicity>,
    child: &mut impl Add<dyn Workable<ThreadId = ThreadIdType>, ChildMultiplicity>,
) -> Result<(), Error> {
    let worker = Get::get(parent)?;
    Add::add(child, worker)?;

    Ok(())
}
