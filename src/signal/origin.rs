//! Track the [Origin] and path of signals passing through the graph.
use std::fmt::Debug;
use std::hash::Hash;

/// [`Origin`] is a trait of a signal type that is passed in from some
/// entry node to the graph. The [Origin] is suppose to
/// provide some unique information about this signal.
///
/// The contract for [Origin] is not fully fleshed out
/// but the [Hash] value for it is used to eliminate
/// signals travelling in a circle.
///
/// **Empty circuit Rule**
///
/// If a signal has travelled in a circle and no other
/// data has been part of that circle as part of the signal
/// traversal then the signal is dropped.
///
/// It is dropped because it has performed an empty cirquit and
/// thus would just repeat its behaviour, assuming all else equal.
///
/// If a signal is chasing nodes that produce some output
/// then it can in theory chase in a circle forever.
pub trait Origin: Debug + Eq + Sync + Send + Hash {}

impl Origin for usize {}

/// Default signal us usually a [crate::Trackable<&static str>]
impl Origin for &'static str {}
