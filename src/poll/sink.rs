use crate::edge::policy::{FollowData, FollowsData};
use crate::graph::{self, BoxAs, Schedule};
use crate::signal::Origin;

/// A poll [`Sink`]: a [graph::Sink] that also [Schedule]s. After a refused `try_push`, it wakes
/// the scheduled waker once it may take more, or on close, so a poll node waits for that instead
/// of blocking its thread.
pub trait Sink: graph::Sink + Schedule {}
impl<T: graph::Sink + Schedule> Sink for T {}

/// A boxed poll [`Sink`], as poll nodes hold their outputs.
pub type DynSink<'params, DataType, SignalType> = Box<
    dyn Sink<DataType = DataType, SignalType = SignalType> + Send + std::marker::Sync + 'params,
>;

impl<'params, DataType, SignalType> FollowsData
    for dyn Sink<DataType = DataType, SignalType = SignalType> + Send + std::marker::Sync + 'params
where
    DataType: 'params,
    SignalType: Origin + 'params,
{
    fn follow_data(this: Box<Self>) -> Box<Self> {
        Box::new(FollowData::new(this))
    }
}

/// Anything that is a poll [`Sink`] boxes as one.
impl<'params, T>
    BoxAs<
        dyn Sink<DataType = T::DataType, SignalType = T::SignalType>
            + Send
            + std::marker::Sync
            + 'params,
    > for T
where
    T: Sink + Send + std::marker::Sync + 'params,
{
    fn box_as(
        self,
    ) -> Box<
        dyn Sink<DataType = T::DataType, SignalType = T::SignalType>
            + Send
            + std::marker::Sync
            + 'params,
    > {
        Box::new(self)
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::mock::{ScheduleOnly, Signal};
    use super::*;
    use crate::graph::{Closeable, TryPush, TryPushable};
    use crate::message::Message;
    use crate::wake::mock::noop_waker;
    use std::sync::atomic::Ordering;

    type PollSink = dyn Sink<DataType = usize, SignalType = Signal> + Send + std::marker::Sync;

    fn following(sink: &ScheduleOnly) -> Box<PollSink> {
        FollowsData::follow_data(Box::new(sink.clone()))
    }

    #[test]
    fn following_forwards_schedule_to_the_inner_sink() {
        let sink = ScheduleOnly::default();
        let mut edge = following(&sink);
        Schedule::schedule(&mut edge, noop_waker());
        assert_eq!(sink.schedules.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn following_drops_a_signal_before_data_on_try_push() {
        let sink = ScheduleOnly::default();
        let mut edge = following(&sink);
        // A signal before any data is dropped, then data goes through.
        assert_eq!(
            edge.try_push(Message::Flush("f".into())).unwrap(),
            TryPush::Pushed
        );
        assert_eq!(edge.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(*sink.items.lock().unwrap(), vec![Message::Data(1)]);
    }

    #[test]
    fn following_forwards_close() {
        let sink = ScheduleOnly::default();
        following(&sink).close().unwrap();
        assert!(sink.closed.load(Ordering::SeqCst));
    }
}
