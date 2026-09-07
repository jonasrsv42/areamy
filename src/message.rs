//! [Message] is the core struct that traverses the computation graph and gets its [Message::Data] [crate::Composable::compose]ed by nodes.

use crate::Origin;
use std::fmt::Debug;

/// A unit of traffic in the computation graph.
///
/// [Message::Data] is transformed by node routines.
/// [Message::Flush] and [Message::Marker] are signals: nodes pass them
/// through and routines never transform them.
///
/// `SignalType` is usually [crate::Trackable], which keeps signals
/// safe in graphs with cycles.
#[derive(Debug, PartialEq)]
pub enum Message<DataType, SignalType>
where
    SignalType: Origin,
{
    /// Payload transformed by each node's routine.
    Data(DataType),
    /// Emit accumulated output, forward the signal, reset state.
    /// Flushing a whole graph needs exactly one active Flush at a time.
    ///
    /// Close contract, per edge: a Flush pushed before close is fully
    /// processed (output, then the Flush, then close) before any node
    /// sees [crate::error::ErrorKind::Closed]. Close without a Flush
    /// keeps nothing. Edges from [crate::make_push] use
    /// [crate::SignalPolicy::FollowData] and drop a signal no data
    /// preceded. Pinned by `tests::close`.
    Flush(SignalType),
    /// Passed along as-is, never enters a routine.
    /// For synchronization and message passing.
    /// Same close ordering as [Message::Flush].
    Marker(SignalType),
}

// Implement Clone for Message conditionally based on whether DataType implements Clone
impl<DataType, SignalType> Clone for Message<DataType, SignalType>
where
    DataType: Clone,
    SignalType: Origin + Clone,
{
    fn clone(&self) -> Self {
        match self {
            Message::Data(data) => Message::Data(data.clone()),
            Message::Flush(signal) => Message::Flush(signal.clone()),
            Message::Marker(signal) => Message::Marker(signal.clone()),
        }
    }
}

impl<DataType, SignalType> Message<DataType, SignalType>
where
    SignalType: Origin,
{
    /// [Message::data_from_iter] provides a convenient method of
    /// extracting [Message::Data] content from an [Iterator] of
    /// [Message]
    pub fn data_from_iter<I>(messages: I) -> Vec<DataType>
    where
        I: Iterator<Item = Message<DataType, SignalType>>,
    {
        let mut a: Vec<DataType> = Vec::new();
        for item in messages {
            match item {
                Message::Data(data) => a.push(data),
                _ => (),
            };
        }
        return a;
    }
}

impl<DataType, SignalType> Message<DataType, SignalType>
where
    SignalType: Origin,
{
    /// [Message::data] provides a convenient method of
    /// extracting [Message::Data] from a [Message]
    pub fn data(self) -> Option<DataType> {
        match self {
            Message::Data(data) => Some(data),
            Message::Flush(_) => None,
            Message::Marker(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_datas_from_iter() {
        let iter = vec![
            Message::Data(1),
            Message::Data(2),
            Message::Flush(3),
            Message::Marker(1),
            Message::Data(5),
        ];
        assert_eq!(Message::data_from_iter(iter.into_iter()), vec![1, 2, 5]);
    }

    #[test]
    fn data_from_message() {
        assert!(matches!(Message::<i32, usize>::Data(5).data(), Some(5)));
        assert!(matches!(Message::<i32, usize>::Flush(5).data(), None));
        assert!(matches!(Message::<i32, usize>::Marker(5).data(), None));
    }
}
