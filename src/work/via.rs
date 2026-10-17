use crate::edge::policy::{PolicyEdge, SignalPolicy};
use crate::error::Error;
use crate::graph::marker::{Multiplicity, Unary};
use crate::graph::{Add, Downstream, Get, Select};
use crate::node::bifurcation::work::node::{Bifurcation, Types as BifurcationTypes};
use crate::node::biunion::work::node::{Biunion, Types as BiunionTypes};
use crate::node::line::work::node::{Line, Types as LineTypes};
use crate::signal::Origin;
use crate::work::{Sink, Workable};
use std::marker::PhantomData;

mod upstream;

pub use upstream::{Sided, Upstream, Whole};

// Owning work edges ([crate::Via]): one impl per downstream type, the upstream's shape (whole or
// moved at a side) dispatched by [Upstream]. A concrete `Self` per impl keeps rustc to one
// candidate, so a data mismatch reads `expected .., found ..`.

/// [Downstream] parameters for a work node taking an [Upstream].
pub struct OwnedWork<ShapeType, SignalType, ThreadIdType>(
    PhantomData<(ShapeType, SignalType, ThreadIdType)>,
);

type UpstreamSink<'params, DataType, SignalType> =
    dyn Sink<DataType = DataType, SignalType = SignalType> + Send + Sync + 'params;

/// The downstream owns the upstream: it schedules it, and the upstream pushes into its input.
///
/// Signals are forwarded ([SignalPolicy::Forward]): the upstream is moved into the downstream, so
/// owning chains can't cycle and forwarding can't loop.
fn adopt<'params, DataType, SignalType, ThreadIdType, ShapeType, DownstreamMultiplicity>(
    upstream: impl Upstream<'params, DataType, SignalType, ThreadIdType, ShapeType>,
    downstream: &mut (
             impl Add<dyn Workable<ThreadId = ThreadIdType> + 'params, DownstreamMultiplicity>
             + Get<UpstreamSink<'params, DataType, SignalType>, DownstreamMultiplicity>
         ),
) -> Result<(), Error>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
    DownstreamMultiplicity: Multiplicity,
{
    let sink = downstream.get()?;
    let upstream = upstream.feed(Box::new(PolicyEdge::new(sink, SignalPolicy::Forward)))?;
    Add::add(downstream, upstream)
}

impl<'params, 'node, T, UpstreamType, ShapeType, DataType, SignalType, ThreadIdType>
    Downstream<UpstreamType, DataType, OwnedWork<ShapeType, SignalType, ThreadIdType>>
    for Line<'node, T>
where
    T: LineTypes,
    Self: Add<dyn Workable<ThreadId = ThreadIdType> + 'params, Unary>
        + Get<UpstreamSink<'params, DataType, SignalType>, Unary>,
    UpstreamType: Upstream<'params, DataType, SignalType, ThreadIdType, ShapeType>,
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    type Out = Self;
    fn own(mut self, upstream: UpstreamType) -> Result<Self, Error> {
        adopt(upstream, &mut self)?;
        Ok(self)
    }
}

impl<'params, 'node, T, UpstreamType, ShapeType, DataType, SignalType, ThreadIdType>
    Downstream<UpstreamType, DataType, OwnedWork<ShapeType, SignalType, ThreadIdType>>
    for Bifurcation<'node, T>
where
    T: BifurcationTypes,
    Self: Add<dyn Workable<ThreadId = ThreadIdType> + 'params, Unary>
        + Get<UpstreamSink<'params, DataType, SignalType>, Unary>,
    UpstreamType: Upstream<'params, DataType, SignalType, ThreadIdType, ShapeType>,
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    type Out = Self;
    fn own(mut self, upstream: UpstreamType) -> Result<Self, Error> {
        adopt(upstream, &mut self)?;
        Ok(self)
    }
}

impl<
    'params,
    'node,
    T,
    MultiplicityType,
    UpstreamType,
    ShapeType,
    DataType,
    SignalType,
    ThreadIdType,
> Downstream<UpstreamType, DataType, OwnedWork<ShapeType, SignalType, ThreadIdType>>
    for Select<Biunion<'node, T>, MultiplicityType>
where
    T: BiunionTypes,
    Biunion<'node, T>: Add<dyn Workable<ThreadId = ThreadIdType> + 'params, MultiplicityType>
        + Get<UpstreamSink<'params, DataType, SignalType>, MultiplicityType>,
    MultiplicityType: Multiplicity,
    UpstreamType: Upstream<'params, DataType, SignalType, ThreadIdType, ShapeType>,
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    type Out = Biunion<'node, T>;
    fn own(self, upstream: UpstreamType) -> Result<Self::Out, Error> {
        let mut node = self.into_node();
        adopt(upstream, &mut node)?;
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use crate::biunion::{Left, Right};
    use crate::graph::Pushable;
    use crate::node::bifurcation::routine::tests::MockBifurcation;
    use crate::node::biunion::routine::tests::MockBiunion;
    use crate::node::line::routine::tests::MockLine;
    use crate::work::{Bifurcation, Biunion, Line};
    use crate::{At, Message, Reads, Tees, Via, Writes, bifurcation};

    #[test]
    fn via_into_each_biunion_side() {
        let left = Line::of(MockLine::new());
        let right = Line::of(MockLine::new());
        let mut left_writer = left.writer().unwrap();
        let mut right_writer = right.writer().unwrap();

        let biunion = Biunion::of(MockBiunion::new());
        let biunion = left.via(biunion.at::<Left>()).unwrap();
        let biunion = right.via(biunion.at::<Right>()).unwrap();
        let mut reader = biunion.reader().unwrap();

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

    /// Both bifurcation sides carry `usize`: the moved side names the owning edge, the borrowed
    /// side a tee.
    #[test]
    fn via_from_a_moved_side() {
        let head = Line::of(MockLine::new());
        let mut writer = head.writer().unwrap();
        let mut router = head.via(Bifurcation::of(MockBifurcation::new())).unwrap();
        let mut right = router.at_mut::<bifurcation::Right>().tee().unwrap();
        let tail = router
            .at::<bifurcation::Left>()
            .via(Line::of(MockLine::new()))
            .unwrap();
        let mut reader = tail.reader().unwrap();

        writer.push(Message::Data(1)).unwrap();

        // Head 1 → 2; left 2 * 2 → tail 8; right 2 * 3 = 6.
        assert_eq!(reader.read().unwrap(), Message::Data(8));
        assert_eq!(right.read().unwrap(), Message::Data(6));
    }

    /// A moved side owned by a moved side: bifurcation Right into biunion Right.
    #[test]
    fn via_from_a_side_into_a_side() {
        let head = Line::of(MockLine::new());
        let left = Line::of(MockLine::new());
        let mut head_writer = head.writer().unwrap();
        let mut left_writer = left.writer().unwrap();

        let mut router = head.via(Bifurcation::of(MockBifurcation::new())).unwrap();
        let mut dropped = router.at_mut::<bifurcation::Left>().tee::<usize>().unwrap();
        let merge = Biunion::of(MockBiunion::new());
        let merge = left.via(merge.at::<Left>()).unwrap();
        let merge = router
            .at::<bifurcation::Right>()
            .via(merge.at::<Right>())
            .unwrap();
        let mut reader = merge.reader().unwrap();

        head_writer.push(Message::Data(1)).unwrap();
        left_writer.push(Message::Data(1)).unwrap();

        let mut outputs = vec![reader.read().unwrap(), reader.read().unwrap()];
        outputs.sort_by_key(|m| match m {
            Message::Data(d) => *d,
            _ => usize::MAX,
        });
        // Left 1 → 2 → 2 * 2; right 1 → 2 → 2 * 3 = 6 → 6 * 3; the biunion adds 0 then 1.
        assert!(
            outputs == vec![Message::Data(4), Message::Data(19)]
                || outputs == vec![Message::Data(5), Message::Data(18)],
            "{outputs:?}"
        );
        assert_eq!(dropped.read().unwrap(), Message::Data(4));
    }

    /// Boxed nodes work at both ends; a boxed downstream stays boxed.
    #[test]
    fn via_through_boxes() {
        let head = Box::new(Line::of(MockLine::new()));
        let mut writer = head.writer().unwrap();
        let tail: Box<Line<'_, _>> = head.via(Box::new(Line::of(MockLine::new()))).unwrap();
        let mut reader = tail.reader().unwrap();

        writer.push(Message::Data(1)).unwrap();

        // 1 → 2 → 4.
        assert_eq!(reader.read().unwrap(), Message::Data(4));
    }
}
