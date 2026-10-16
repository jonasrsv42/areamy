use crate::graph::marker::Connection;

/// [`BoxAs`] lifts a concrete type into dynamic dispatch: it boxes `Self` as the `dyn` connection
/// `ConnectionType` (unsizing to a generic target, which stable Rust can't name). Each `dyn` kind
/// implements it once, as a blanket over everything that is that kind, so one generic
/// [crate::graph::Get] per child serves every kind its edge is.
///
/// Only `dyn` kinds with a blanket are targets; anything else doesn't compile:
///
/// ```compile_fail,E0277
/// use areamy::graph::Get;
/// use areamy::sync::{Receiver, Sender};
/// let receiver = Receiver::<usize, areamy::Trackable<&'static str>>::new();
/// let _ = Get::<Sender<usize, areamy::Trackable<&'static str>>>::get(&receiver);
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be lifted into `{ConnectionType}`",
    label = "only `dyn work::Sink` or `dyn poll::Sink` (+ Send + Sync) are lift targets"
)]
pub trait BoxAs<ConnectionType: Connection + ?Sized> {
    fn box_as(self) -> Box<ConnectionType>;
}
