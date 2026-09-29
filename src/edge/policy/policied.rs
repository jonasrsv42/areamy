use crate::edge::policy::SignalPolicy;
use crate::graph::Outlet;

/// A sink type that can wrap itself in a [crate::PolicyEdge]. Implemented on each concrete dyn
/// sink ([crate::work::Sink], [crate::poll::Sink]): a generic caller only knows an abstract
/// `Box<Self>`, which can't be unsized into, so the wrap happens where `Self` is concrete.
pub trait Policied: Outlet {
    fn with_policy(this: Box<Self>, policy: SignalPolicy) -> Box<Self>;
}
