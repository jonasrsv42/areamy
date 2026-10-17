use crate::graph::Outlet;

/// A sink type that can wrap itself in a [super::FollowData]. Implemented on each concrete dyn
/// sink ([crate::work::Sink], [crate::poll::Sink]): a generic caller only knows an abstract
/// `Box<Self>`, which can't be unsized into, so the wrap happens where `Self` is concrete.
pub trait FollowsData: Outlet {
    fn follow_data(this: Box<Self>) -> Box<Self>;
}
