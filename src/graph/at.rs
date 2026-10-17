use crate::graph::marker::{Connection, Multiplicity};

mod select;
mod select_mut;
mod select_ref;

pub use select::Select;
pub use select_mut::SelectMut;
pub use select_ref::SelectRef;

/// [`At`] names one [Multiplicity] of a node, so connections reach a specific side.
///
/// - [At::at] moves the node: only an owning edge ([crate::Via::via]) takes it.
/// - [At::at_mut] borrows it to add to: the upstream of [crate::Pushes::pushes], the downstream
///   of [crate::Schedules::schedules], [crate::Tees::tee].
/// - [At::at_ref] borrows it to get from: the downstream of [crate::Pushes::pushes],
///   [crate::Writes::writer].
///
/// Single-sided nodes never need any of them.
///
/// ```ignore
/// router.at_mut::<Right>().pushes(&noise)?;
/// up.pushes(&merge.at_ref::<Left>())?;
/// let speech = router.at::<Left>().via(encoder)?;
/// let merge = text.via(merge.at::<Right>())?;
/// ```
pub trait At: Sized {
    fn at<MultiplicityType>(self) -> Select<Self, MultiplicityType> {
        Select::new(self)
    }

    fn at_mut<MultiplicityType: Multiplicity>(&mut self) -> SelectMut<'_, Self, MultiplicityType> {
        SelectMut::new(self)
    }

    fn at_ref<MultiplicityType: Multiplicity>(&self) -> SelectRef<'_, Self, MultiplicityType> {
        SelectRef::new(self)
    }
}

// Bounded on the impl, so `&mut` bindings and guards autoderef to the node.
impl<NodeType: Connection> At for NodeType {}
