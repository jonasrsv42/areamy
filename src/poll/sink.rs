use crate::edge::policy::{Policied, PolicyEdge, SignalPolicy};
use crate::graph;
use crate::poll::Room;
use crate::signal::Origin;

/// A poll [`Sink`]: a [graph::Sink] that can also report [Room], so a poll node waits for room
/// instead of blocking its thread.
pub trait Sink: graph::Sink + Room {}
impl<T: graph::Sink + Room> Sink for T {}

impl<'params, DataType, SignalType> Policied
    for dyn Sink<DataType = DataType, SignalType = SignalType> + Send + std::marker::Sync + 'params
where
    DataType: 'params,
    SignalType: Origin + 'params,
{
    fn with_policy(this: Box<Self>, policy: SignalPolicy) -> Box<Self> {
        Box::new(PolicyEdge::new(this, policy))
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::mock::{RoomOnly, Signal};
    use super::*;
    use crate::graph::{Closeable, TryPush, TryPushable};
    use crate::message::Message;
    use crate::poll::waker::mock::noop_waker;
    use core::task::Poll;
    use std::sync::atomic::Ordering;

    type PollSink = dyn Sink<DataType = usize, SignalType = Signal> + Send + std::marker::Sync;

    fn policied(sink: &RoomOnly) -> Box<PollSink> {
        Policied::with_policy(Box::new(sink.clone()), SignalPolicy::FollowData)
    }

    #[test]
    fn policied_forwards_room_to_the_inner_sink() {
        let sink = RoomOnly::default();
        let mut edge = policied(&sink);
        assert_eq!(
            Room::poll(&mut edge, &mut noop_waker()).unwrap(),
            Poll::Pending
        );
        assert_eq!(sink.polls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn policied_applies_the_policy_on_try_push() {
        let sink = RoomOnly::default();
        let mut edge = policied(&sink);
        // FollowData: a signal before any data is dropped, then data goes through.
        assert_eq!(
            edge.try_push(Message::Flush("f".into())).unwrap(),
            TryPush::Pushed
        );
        assert_eq!(edge.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(*sink.items.lock().unwrap(), vec![Message::Data(1)]);
    }

    #[test]
    fn policied_forwards_close() {
        let sink = RoomOnly::default();
        policied(&sink).close().unwrap();
        assert!(sink.closed.load(Ordering::SeqCst));
    }
}
