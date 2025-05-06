//! [EXPPERIMENTAL] Track signals in graph using [Trackable].
use crate::Origin;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// [`Trackable`] is a signal type that encapsulates the usual
/// user provided [`Origin`] signal.
///
/// [Trackable] adds reference counting to a [Origin] it is used
/// as a means of aiding synchronization via signal tracking.
///
/// The graph synchronization feature
/// via signals is still very **Experimental** and under development.
///
/// Illustating with an example, Imagine the following graph with a circular
/// push path.
///
/// ```bash
///   _____________
///   v            |
///  Node (1) -> Node (2) -> Out
/// ```
/// When a signal is passed it will fork at Node (2)
/// one will enter the Out leaf and one traverse back to Node (1).
///
/// If data also traversed across the [crate::Pushable] connection as part of for example
/// a [crate::Message::Flush] we do not want to accept the first instance of the signal entering
/// `Out` as a `Complete` flush. Because the `Flush` may have triggered some new output going into
/// Node (1) and when Node (1) is [crate::Message::Flush] by the signal chasing it (Or even by it
/// just recieving the new data) it may produce new output for Node (2) and the cycle may repeat.
///
/// The idea is that when we are [crate::Message::Flush]ing we don't just want to wait to recieve the
/// signal at our output node. But we want to wait until the signal has chased around the graph and
/// the graph is no longer transmitting data as part of the signal traversal.
///
/// For this to work as intended there should only be a [crate::Message::Flush] signal of
/// the same origin at the same time. If there's multiple signals or if data is actively being
/// pushed into the graph during the [crate::Message::Flush] then behaviour is not entirely defined
/// yet.
#[derive(Debug)]
pub struct Trackable<OriginType>
where
    OriginType: Origin + Hash,
{
    origin: Arc<OriginType>,
    active: Arc<AtomicUsize>,
}

/// Implement reference incrementing for [Trackable]
impl<OriginType> Clone for Trackable<OriginType>
where
    OriginType: Origin + Hash,
{
    fn clone(&self) -> Self {
        self.active.fetch_add(1, Ordering::Relaxed);
        Trackable {
            origin: self.origin.clone(),
            active: self.active.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        *self = source.clone()
    }
}

/// Implement reference decrementing for [Trackable]
impl<OriginType> Drop for Trackable<OriginType>
where
    OriginType: Origin + Hash,
{
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<OriginType> Trackable<OriginType>
where
    OriginType: Clone + Origin + Hash + 'static,
{
    pub fn new(origin: OriginType) -> Self {
        Trackable {
            origin: Arc::new(origin),
            active: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

impl Into<Trackable<&'static str>> for &'static str {
    fn into(self) -> Trackable<&'static str> {
        Trackable::new(self)
    }
}

/// [Trackable] needs to expose the [Hash] trait
/// of origin to keep supporing the loop-breaking
/// behaviour.
impl<OriginType> Hash for Trackable<OriginType>
where
    OriginType: Origin,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Hash::hash(&self.origin, state)
    }
}
impl<OriginType> PartialEq for Trackable<OriginType>
where
    OriginType: Origin,
{
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin
    }
}

impl<OriginType> Eq for Trackable<OriginType> where OriginType: Origin {}
impl<OriginType> Origin for Trackable<OriginType> where OriginType: Origin + Clone + 'static {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::bifurcation;
    use crate::node::bifurcation::routine::tests::MockBifurcation;
    use crate::{sink::work::tee, work::Source};
    use crate::{work::make_bifurcation, DefaultThread, Message, Pushable, Workable};

    /// We need many more tests here to ensure Tracking behaves reasonably in semi-complex graphs! :)

    #[test]
    fn trackable_signal_tracks_active() {
        let mut bifur = make_bifurcation(Ok(MockBifurcation::new())).unwrap();

        let mut source = Source::new(&bifur).unwrap();

        let mut left_sink = tee::Sink::new::<bifurcation::Left>(&mut bifur).unwrap();
        let mut right_sink = tee::Sink::new::<bifurcation::Right>(&mut bifur).unwrap();

        let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = bifur;

        let hello_track = Trackable::new("hello");

        // It is the only one so it's final.
        assert_eq!(hello_track.active(), 1);

        // Add one flush
        source.push(Message::Flush(hello_track.clone())).unwrap();

        assert_eq!(hello_track.active(), 2);
        workable.work().unwrap();

        {
            let _signal = left_sink.read().unwrap();

            // There is 3 instances alive now. One inside the signal variable above and one inside
            // the output of the right_sink and the original one.
            assert_eq!(hello_track.active(), 3);
        }

        // Now 2 cause we dropped the last _signal
        assert_eq!(hello_track.active(), 2);

        {
            let _signal = right_sink.read().unwrap();

            // There is 2 instances alive now. This one and the original one.
            assert_eq!(hello_track.active(), 2);
        }

        // Now only original instance is alive.
        assert_eq!(hello_track.active(), 1);
    }
}
