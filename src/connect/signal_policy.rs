//! Signal policy wrappers for controlling how signals are propagated through the graph.
use crate::error::Error;
use crate::marker::Connection;
use crate::message::Message;
use crate::{Closeable, Pushable, Sink};

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
    use crate::connect::sync::Receiver;
    use crate::{Message, Origin};

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestSignal(u32);

    impl Origin for TestSignal {}

    #[test]
    fn test_policy_edge_forward() {
        let edge = Receiver::<f64, TestSignal>::new();
        let mut policy_edge = PolicyEdge::new(edge.sender(), SignalPolicy::Forward);

        // Forward policy should allow all signals
        policy_edge.push(Message::Flush(TestSignal(1))).unwrap();
        policy_edge.push(Message::Marker(TestSignal(2))).unwrap();
        policy_edge.push(Message::Data(3.5)).unwrap();
        policy_edge.push(Message::Flush(TestSignal(3))).unwrap();

        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0], Message::Flush(TestSignal(1)));
        assert_eq!(messages[1], Message::Marker(TestSignal(2)));
        assert_eq!(messages[2], Message::Data(3.5));
        assert_eq!(messages[3], Message::Flush(TestSignal(3)));
    }

    #[test]
    fn test_policy_edge_follow_data() {
        let edge = Receiver::<f64, TestSignal>::new();
        let mut policy_edge = PolicyEdge::new(edge.sender(), SignalPolicy::FollowData);

        // First signal should be dropped (no data yet)
        policy_edge.push(Message::Flush(TestSignal(1))).unwrap();

        // Add data
        policy_edge.push(Message::Data(3.5)).unwrap();

        // Now signal should be forwarded
        policy_edge.push(Message::Marker(TestSignal(2))).unwrap();

        // This signal should be dropped (previous was a signal)
        policy_edge.push(Message::Flush(TestSignal(3))).unwrap();

        // Add more data
        policy_edge.push(Message::Data(4.0)).unwrap();

        // Now signal should be forwarded again
        policy_edge.push(Message::Flush(TestSignal(4))).unwrap();

        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0], Message::Data(3.5));
        assert_eq!(messages[1], Message::Marker(TestSignal(2)));
        assert_eq!(messages[2], Message::Data(4.0));
        assert_eq!(messages[3], Message::Flush(TestSignal(4)));
    }

    #[test]
    fn test_policy_edge_block() {
        let edge = Receiver::<f64, TestSignal>::new();
        let mut policy_edge = PolicyEdge::new(edge.sender(), SignalPolicy::Block);

        // All signals should be blocked
        policy_edge.push(Message::Flush(TestSignal(1))).unwrap();
        policy_edge.push(Message::Marker(TestSignal(2))).unwrap();

        // Data should still go through
        policy_edge.push(Message::Data(3.5)).unwrap();
        policy_edge.push(Message::Data(4.0)).unwrap();

        // More signals should be blocked
        policy_edge.push(Message::Flush(TestSignal(3))).unwrap();

        let messages = edge.read_all().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], Message::Data(3.5));
        assert_eq!(messages[1], Message::Data(4.0));
    }
}
