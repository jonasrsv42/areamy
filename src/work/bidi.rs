use crate::edge::policy::{PolicyEdge, SignalPolicy};
use crate::error::Error;
use crate::graph::marker::Multiplicity;
use crate::graph::{Add, Get};
use crate::signal::Origin;
use crate::work::Sink;
use crate::work::{Schedule, Workable};
use std::marker::PhantomData;

/// [`Bidi`] is [crate::Push] + [Schedule]. See [Bidi::connect].
///
/// The data type is an optional annotation (`work::Bidi::<T>::connect`); the signal type is
/// always inferred. Name a child side with [crate::graph::At::at].
///
/// ```ignore
/// work::Bidi::connect(parent, &mut child)?;
/// work::Bidi::connect(parent, &mut biunion.at::<Right>())?;
/// ```
pub struct Bidi<DataType>(PhantomData<DataType>);

impl<DataType: Send + Sync + 'static> Bidi<DataType> {
    /// [`Bidi::connect`] creates a `bidi` connection between two nodes.
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
    pub fn connect<
        'params,
        ParentType,
        ParentMultiplicity: Multiplicity,
        ChildMultiplicity: Multiplicity,
        SignalType: Origin + Send + Sync + 'static,
        ThreadIdType,
    >(
        mut parent: ParentType,
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
        let sink = child.get()?;
        Add::add(
            &mut parent,
            Box::new(PolicyEdge::new(sink, SignalPolicy::Forward)),
        )?;
        Schedule::connect(parent, child)
    }
}

#[cfg(test)]
mod tests {
    use crate::biunion::{Left, Right};
    use crate::graph::Pushable;
    use crate::node::biunion::routine::tests::MockBiunion;
    use crate::node::line::routine::tests::MockLine;
    use crate::work::{self, Biunion, Line, Reader, Writer};
    use crate::{At, Message};

    #[test]
    fn bidi_into_each_biunion_side() {
        let left = Line::of(MockLine::new());
        let right = Line::of(MockLine::new());
        let mut left_writer = Writer::new(&left).unwrap();
        let mut right_writer = Writer::new(&right).unwrap();

        let mut biunion = Biunion::of(MockBiunion::new());
        work::Bidi::connect(left, &mut biunion.at::<Left>()).unwrap();
        work::Bidi::connect(right, &mut biunion.at::<Right>()).unwrap();
        let mut reader = Reader::new(biunion).unwrap();

        left_writer.push(Message::Data(1)).unwrap();
        right_writer.push(Message::Data(1)).unwrap();

        let mut outputs = vec![reader.read().unwrap(), reader.read().unwrap()];
        outputs.sort_by_key(|m| match m {
            Message::Data(d) => *d,
            _ => usize::MAX,
        });
        // Left: 2 * 2 + 0, right: 2 * 3 + 1 (MockLine doubles first).
        assert_eq!(outputs, vec![Message::Data(4), Message::Data(7)]);
    }
}
