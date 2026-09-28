//! Signal policy wrappers for controlling how signals are propagated through the graph.
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Closeable, Pushable, Sink, TryPush};
use crate::message::Message;

#[derive(Debug)]
/// Policy for handling signals in the queue
pub enum SignalPolicy {
    /// Always forward signals into the queue
    Forward,
    /// Only forward signals if the last entry was not a signal
    FollowData,
    /// Never forward signals into the queue
    Block,
}

/// A wrapper around a `Sink` that applies a signal policy.
/// This allows for different signal policies to be applied to the same
/// underlying queue when pushed to from different parents.
#[derive(Debug)]
pub struct PolicyEdge<SinkType>
where
    SinkType: Sink,
{
    /// The underlying pushable that messages will be sent to
    inner: SinkType,
    /// The policy to apply when pushing messages
    policy: SignalPolicy,
    /// Tracks if the last message was data (needed for FollowData policy)
    last_was_data: bool,
}

impl<SinkType> PolicyEdge<SinkType>
where
    SinkType: Sink,
{
    /// Create a new policy wrapper with the specified policy
    pub fn new(inner: SinkType, policy: SignalPolicy) -> Self {
        Self {
            inner,
            policy,
            last_was_data: false,
        }
    }

    /// Helper method to determine if a message should be forwarded based on the policy
    fn should_forward(&mut self, is_signal: bool) -> bool {
        if is_signal {
            match self.policy {
                SignalPolicy::Forward => {
                    // Always forward signals
                    self.last_was_data = false;
                    true
                }
                SignalPolicy::FollowData => {
                    // Only forward if the last entry was data
                    if self.last_was_data {
                        self.last_was_data = false;
                        true
                    } else {
                        // Skip this signal
                        false
                    }
                }
                SignalPolicy::Block => {
                    // Never forward signals
                    false
                }
            }
        } else {
            // Always forward data messages
            self.last_was_data = true;
            true
        }
    }
}

impl<SinkType> Connection for PolicyEdge<SinkType> where SinkType: Sink {}

impl<SinkType> Pushable for PolicyEdge<SinkType>
where
    SinkType: Sink,
{
    type DataType = SinkType::DataType;
    type SignalType = SinkType::SignalType;

    fn push(&mut self, message: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        // Check if the message is a signal
        let is_signal = match &message {
            Message::Data(_) => false,
            Message::Flush(_) | Message::Marker(_) => true,
        };

        // Apply policy for signals
        if self.should_forward(is_signal) {
            self.inner.push(message)?;
        }

        Ok(())
    }

    /// A message the policy drops counts as pushed. On `Full` the policy state is rolled back,
    /// so the retry is judged the same way.
    fn try_push(
        &mut self,
        message: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error> {
        let is_signal = !matches!(message, Message::Data(_));
        let last_was_data = self.last_was_data;
        if !self.should_forward(is_signal) {
            return Ok(TryPush::Pushed);
        }
        let result = self.inner.try_push(message)?;
        if matches!(result, TryPush::Full(_)) {
            self.last_was_data = last_was_data;
        }
        Ok(result)
    }
}

impl<SinkType> Closeable for PolicyEdge<SinkType>
where
    SinkType: Sink,
{
    fn close(&mut self) -> Result<(), Error> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Message;
    use crate::Trackable;
    use crate::edge::sync::Receiver;
    use crate::graph::tests::Bounded;

    type TestSignal = Trackable<&'static str>;

    #[test]
    fn try_push_hands_back_when_full() {
        let mut edge = PolicyEdge::new(Bounded::new(1), SignalPolicy::Forward);
        assert_eq!(edge.try_push(Message::Data(1)).unwrap(), TryPush::Pushed);
        assert_eq!(
            edge.try_push(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
    }

    #[test]
    fn try_push_full_signal_is_forwarded_on_retry() {
        let mut edge = PolicyEdge::new(Bounded::new(1), SignalPolicy::FollowData);
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
    fn try_push_dropped_signal_counts_as_pushed() {
        let mut edge = PolicyEdge::new(Bounded::new(1), SignalPolicy::FollowData);
        assert_eq!(
            edge.try_push(Message::Flush("f".into())).unwrap(),
            TryPush::Pushed
        );
        assert!(edge.inner.items.is_empty());
    }

    #[test]
    fn test_policy_edge_forward() {
        let edge = Receiver::<f64, TestSignal>::new();
        let mut policy_edge = PolicyEdge::new(edge.sender(), SignalPolicy::Forward);

        // Forward policy should allow all signals
        policy_edge.push(Message::Flush("1".into())).unwrap();
        policy_edge.push(Message::Marker("2".into())).unwrap();
        policy_edge.push(Message::Data(3.5)).unwrap();
        policy_edge.push(Message::Flush("3".into())).unwrap();

        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0], Message::Flush("1".into()));
        assert_eq!(messages[1], Message::Marker("2".into()));
        assert_eq!(messages[2], Message::Data(3.5));
        assert_eq!(messages[3], Message::Flush("3".into()));
    }

    #[test]
    fn test_policy_edge_follow_data() {
        let edge = Receiver::<f64, TestSignal>::new();
        let mut policy_edge = PolicyEdge::new(edge.sender(), SignalPolicy::FollowData);

        // First signal should be dropped (no data yet)
        policy_edge.push(Message::Flush("1".into())).unwrap();

        // Add data
        policy_edge.push(Message::Data(3.5)).unwrap();

        // Now signal should be forwarded
        policy_edge.push(Message::Marker("2".into())).unwrap();

        // This signal should be dropped (previous was a signal)
        policy_edge.push(Message::Flush("3".into())).unwrap();

        // Add more data
        policy_edge.push(Message::Data(4.0)).unwrap();

        // Now signal should be forwarded again
        policy_edge.push(Message::Flush("4".into())).unwrap();

        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0], Message::Data(3.5));
        assert_eq!(messages[1], Message::Marker("2".into()));
        assert_eq!(messages[2], Message::Data(4.0));
        assert_eq!(messages[3], Message::Flush("4".into()));
    }

    #[test]
    fn test_policy_edge_block() {
        let edge = Receiver::<f64, TestSignal>::new();
        let mut policy_edge = PolicyEdge::new(edge.sender(), SignalPolicy::Block);

        // All signals should be blocked
        policy_edge.push(Message::Flush("1".into())).unwrap();
        policy_edge.push(Message::Marker("2".into())).unwrap();

        // Data should still go through
        policy_edge.push(Message::Data(3.5)).unwrap();
        policy_edge.push(Message::Data(4.0)).unwrap();

        // More signals should be blocked
        policy_edge.push(Message::Flush("3".into())).unwrap();

        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], Message::Data(3.5));
        assert_eq!(messages[1], Message::Data(4.0));
    }
}
