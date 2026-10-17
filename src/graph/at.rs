use crate::graph::marker::{Connection, Multiplicity};

mod select;
mod select_mut;
mod select_ref;

pub use select::Select;
pub use select_mut::SelectMut;
pub use select_ref::SelectRef;

/// [`At`] names one [Multiplicity] of a node, so connections reach a specific side.
///
/// - [At::at] moves the node: only owning edges take it: [crate::Via::via] and
///   [crate::Reads::reader].
/// - [At::at_mut] borrows it to add to: the upstream of [crate::Pushes::pushes], the downstream
///   of [crate::Schedules::schedules], [crate::Tees::tee].
/// - [At::at_ref] borrows it to get from: the downstream of [crate::Pushes::pushes],
///   [crate::Writes::writer].
///
/// Single-sided nodes never need any of them. A verb on the wrong one, like
/// `node.at::<Left>().pushes(..)`, reads "the method `pushes` exists ... but its trait bounds
/// were not satisfied": swap to the borrowing selector the verb takes.
///
/// ```ignore
/// router.at_mut::<Right>().pushes(&noise)?;
/// up.pushes(&merge.at_ref::<Left>())?;
/// let speech = router.at::<Left>().via(encoder)?;
/// let merge = text.via(merge.at::<Right>())?;
/// ```
pub trait At: Sized {
    /// Moves the node, naming one side.
    fn at<MultiplicityType: Multiplicity>(self) -> Select<Self, MultiplicityType> {
        Select::new(self)
    }

    /// Borrows the node to add to one side.
    fn at_mut<MultiplicityType: Multiplicity>(&mut self) -> SelectMut<'_, Self, MultiplicityType> {
        SelectMut::new(self)
    }

    /// Borrows the node to get from one side.
    fn at_ref<MultiplicityType: Multiplicity>(&self) -> SelectRef<'_, Self, MultiplicityType> {
        SelectRef::new(self)
    }
}

// Bounded on the impl, so `&mut` bindings and guards autoderef to the node.
impl<NodeType: Connection> At for NodeType {}

#[cfg(test)]
mod tests {
    use crate::biunion::Left;
    use crate::node::biunion::routine::tests::MockBiunion;
    use crate::node::biunion::work::node::Node as BiunionNode;
    use crate::node::line::routine::tests::MockLine;
    use crate::node::line::work::node::Node as LineNode;
    use crate::work::{Biunion, Line};
    use crate::{At, DefaultThread, Schedules, Tees, Trackable, Writes};
    use std::sync::Mutex;

    type Signal = Trackable<&'static str>;
    type TestLine = Line<'static, LineNode<usize, usize, Signal, DefaultThread, MockLine>>;
    type TestBiunion =
        Biunion<'static, BiunionNode<usize, usize, usize, Signal, DefaultThread, MockBiunion>>;

    /// Endpoints resolve to the node through a `&mut` binding, as in a subgraph fn.
    #[test]
    fn endpoints_through_a_mut_binding() {
        fn wire(line: &mut TestLine, merge: &mut TestBiunion) {
            let _writer = line.writer::<usize>().unwrap();
            let _tee = line.tee::<usize>().unwrap();
            let _left = merge.at_ref::<Left>().writer::<usize>().unwrap();
            let up: TestLine = Line::of(MockLine::new());
            line.schedules(up).unwrap();
        }
        wire(
            &mut Line::of(MockLine::new()),
            &mut Biunion::of(MockBiunion::new()),
        );
    }

    /// Endpoints resolve to the node through a `MutexGuard`.
    #[test]
    fn endpoints_through_a_guard() {
        let line: Mutex<TestLine> = Mutex::new(Line::of(MockLine::new()));
        let _writer = line.lock().unwrap().writer::<usize>().unwrap();
        let _tee = line.lock().unwrap().tee::<usize>().unwrap();
        let up: TestLine = Line::of(MockLine::new());
        line.lock().unwrap().schedules(up).unwrap();
    }
}
