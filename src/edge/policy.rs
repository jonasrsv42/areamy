//! Signal policies: edges that decide which signals pass. [FollowData] is the one a `pushes`
//! side path pushes through.
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::marker::Multiplicity;
use crate::graph::{Closeable, Outlet, Pushable, Schedule, TryPush, TryPushable};
use crate::message::Message;
use crate::wake;

mod follows;

pub use follows::FollowsData;

/// Wraps a sink so a signal passes only right behind data: a signal with no data before it, or
/// right after another signal, is dropped. A `pushes` side path may be a back-edge into its own
/// upstream, and this keeps a signal from circling the loop forever.
///
/// Each capability is forwarded only when the inner sink has it, so the same wrapper serves
/// work and poll sinks.
#[derive(Debug)]
pub struct FollowData<SinkType> {
    inner: SinkType,
    /// The last message passed was data, so a signal may follow it.
    last_was_data: bool,
}

impl<SinkType> FollowData<SinkType> {
    pub fn new(inner: SinkType) -> Self {
        Self {
            inner,
            last_was_data: false,
        }
    }

    /// Whether `message` passes, advancing the follow state.
    fn passes<DataType, SignalType: crate::signal::Origin>(
        &mut self,
        message: &Message<DataType, SignalType>,
    ) -> bool {
        let is_data = message.as_data().is_some();
        let passes = is_data || self.last_was_data;
        self.last_was_data = is_data;
        passes
    }
}

impl<SinkType> Connection for FollowData<SinkType> {}

impl<SinkType> Pushable for FollowData<SinkType>
where
    SinkType: Pushable,
{
    fn push(&mut self, message: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        if self.passes(&message) {
            self.inner.push(message)?;
        }
        Ok(())
    }
}

impl<SinkType> Outlet for FollowData<SinkType>
where
    SinkType: Outlet,
{
    type DataType = SinkType::DataType;
    type SignalType = SinkType::SignalType;
}

impl<SinkType> TryPushable for FollowData<SinkType>
where
    SinkType: TryPushable,
{
    /// Same rule as [FollowData::push], but the inner sink may hand the message back, so the
    /// follow state must not advance for a message that wasn't delivered.
    fn try_push(
        &mut self,
        message: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error> {
        // `passes` updates `last_was_data`; keep the old value to undo it on a refusal.
        let last_was_data = self.last_was_data;

        // A dropped signal is consumed, same as `push` returning Ok.
        if !self.passes(&message) {
            return Ok(TryPush::Pushed);
        }

        let result = self.inner.try_push(message)?;

        // Full or Stuck: the message comes back for a retry, so restore the state it was judged
        // under. Otherwise a refused Flush would be dropped on retry, because the edge would
        // think the last message was a signal.
        if !matches!(result, TryPush::Pushed) {
            self.last_was_data = last_was_data;
        }

        Ok(result)
    }
}

impl<SinkType> Closeable for FollowData<SinkType>
where
    SinkType: Closeable,
{
    fn close(&mut self) -> Result<(), Error> {
        self.inner.close()
    }
}

/// Scheduling is the inner sink's: the edge only drops signals, it never holds messages, so it
/// never refuses on its own.
impl<SinkType, MultiplicityType> Schedule<MultiplicityType> for FollowData<SinkType>
where
    SinkType: Schedule<MultiplicityType>,
    MultiplicityType: Multiplicity,
{
    fn schedule(&mut self, waker: wake::Waker) {
        Schedule::<MultiplicityType>::schedule(&mut self.inner, waker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Message;
    use crate::Trackable;
    use crate::edge::sync::Receiver;
    use crate::graph::tests::Bounded;
    use crate::tests::support::{DEADLINE, deadline};
    use core::num::NonZeroUsize;

    type TestSignal = Trackable<&'static str>;

    #[test]
    fn try_push_hands_back_when_full() {
        let mut edge = FollowData::new(Bounded::new(1));
        assert_eq!(edge.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(
            edge.try_push(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
    }

    #[test]
    fn try_push_full_signal_is_forwarded_on_retry() {
        let mut edge = FollowData::new(Bounded::new(1));
        assert_eq!(edge.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);

        let TryPush::Full(flush) = edge.try_push(Message::Flush("f".into())).unwrap() else {
            panic!("expected Full");
        };
        edge.inner.items.clear();

        // The refused Flush still follows data, so the retry forwards it.
        assert_eq!(edge.try_push(flush).unwrap(), TryPush::Pushed);
        assert_eq!(edge.inner.items, vec![Message::Flush("f".into())]);
    }

    #[test]
    fn try_push_stuck_signal_is_forwarded_on_retry() {
        let rx = Receiver::<f64, TestSignal>::bounded(NonZeroUsize::MIN);
        let mut edge = FollowData::new(rx.sender());
        assert_eq!(edge.try_push(Message::Data(1.0)).unwrap(), TryPush::Pushed);

        let TryPush::Full(flush) = edge.try_push(Message::Flush("f".into())).unwrap() else {
            panic!("expected Full");
        };
        let TryPush::Stuck(flush) = edge.try_push(flush).unwrap() else {
            panic!("expected Stuck");
        };
        rx.read_front().unwrap();

        // Refused twice, still follows data, so the retry forwards it.
        assert_eq!(edge.try_push(flush).unwrap(), TryPush::Pushed);
        // `poll`, not `read_front`: a dropped Flush must fail here, not hang.
        assert_eq!(rx.poll().unwrap(), Some(Message::Flush("f".into())));
    }

    #[test]
    fn try_push_dropped_signal_counts_as_pushed() {
        let mut edge = FollowData::new(Bounded::new(1));
        assert_eq!(
            edge.try_push(Message::Flush("f".into())).unwrap(),
            TryPush::Pushed
        );
        assert!(edge.inner.items.is_empty());
    }

    #[test]
    fn a_signal_passes_only_right_behind_data() {
        let _deadline = deadline(DEADLINE);
        let edge = Receiver::<f64, TestSignal>::new();
        let mut follow = FollowData::new(edge.sender());

        // No data yet: dropped.
        follow.push(Message::Flush("1".into())).unwrap();
        follow.push(Message::Data(3.5)).unwrap();
        // Right behind data: passes.
        follow.push(Message::Marker("2".into())).unwrap();
        // Right behind a signal: dropped.
        follow.push(Message::Flush("3".into())).unwrap();
        follow.push(Message::Data(4.0)).unwrap();
        follow.push(Message::Flush("4".into())).unwrap();

        assert_eq!(
            edge.read_all().unwrap(),
            vec![
                Message::Data(3.5),
                Message::Marker("2".into()),
                Message::Data(4.0),
                Message::Flush("4".into()),
            ]
        );
    }
}
