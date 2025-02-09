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
/// * `parent` - A type that we can retrieve a (Workable) from and add a (Pushable) into. The
///     (Workable) will be added to the child so the child can schedule the work. The (Pushable)
///     will be added as output for the child so it can push into the parent. The method takes
///     a (&mut) because the (Add) method requires a mutable reference.
///
/// * `child` - A type that we can add the parent (Workable) into and from which we can retrieve the
///     (Pushable) and give to the parent.
pub fn make_bidi<
    ChildMultiplicity: Multiplicity, // Generic over type of child connection
    ParentMultiplicity: Multiplicity, // Generic over type of parent connection
    MessageType,
    ThreadIdType,
>(
    parent: &mut (impl Add<dyn Pushable<Message = MessageType>, ParentMultiplicity>
              + Get<dyn Workable<ThreadId = ThreadIdType>, ParentMultiplicity>),
    child: &mut (impl Add<dyn Workable<ThreadId = ThreadIdType>, ChildMultiplicity>
              + Get<dyn Pushable<Message = MessageType>, ChildMultiplicity>),
) -> Result<(), Error> {
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
