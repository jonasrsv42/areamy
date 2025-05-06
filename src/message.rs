//! [Message] is the core struct that traverses the computation graph and gets its [Message::Data] [crate::Composable::compose]ed by nodes.

use crate::Origin;
use std::fmt::Debug;

/// [`Message`] data types that flow through our computational graph.
/// A [Message] will either hold [Message::Data] or a signal variant.
///
/// The [Message] will be transformed through computation
/// signals such as [Message::Flush] or [Message::Marker] can
/// be used for synchronization or message passing and will
/// not be transformed by computation routines.
///
/// The SignalType Generic of the [Message::Flush] and [Message::Marker] can be used
/// for message passing. It is usually of instance [crate::Trackable] to allow proper
/// signal passing even in loopy graphs.
#[derive(Debug, PartialEq)]
pub enum Message<DataType, SignalType>
where
    DataType: Sync + Send,
    SignalType: Origin + Sync + Send,
{
    /// [Message::Data] will be traversing through the graph and be transformed
    Data(DataType),
    /// [Message::Flush] is a signal for forcing output of accumulated state, if possible, and
    /// clearing the state of the graph in the signals path.
    ///
    /// When a node receives [Message::Flush] it should emit any existing result
    /// it has and reset it's computation state.
    ///
    /// [Message::Flush] can be used to flush a whole graph, but will only work properly if
    /// there's only one active [Message::Flush] in the graph until it is finished.
    Flush(SignalType),
    /// [Message::Marker] should be passed a long by graph nodes, it will never enter computation routines and
    /// can be used as a type of message passing and/or synchronization.
    Marker(SignalType),
}

// Implement Clone for Message conditionally based on whether DataType implements Clone
impl<DataType, SignalType> Clone for Message<DataType, SignalType>
where
    DataType: Clone + Sync + Send,
    SignalType: Origin + Clone + Sync + Send,
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
    DataType: Sync + Send,
    SignalType: Origin + Sync + Send,
{
    /// [Message::data_from_iter] provides a convenient method of
    /// extracting [Message::Data] content from an [Iterator] of
    /// [Message]
    pub fn data_from_iter<'a, I>(messages: I) -> Vec<DataType>
    where
        DataType: Clone + Sync + Send + 'a,
        SignalType: Origin + Sync + Send + 'a,
        I: Iterator<Item = &'a Message<DataType, SignalType>>,
    {
        let mut a: Vec<DataType> = Vec::new();
        for item in messages {
            match item {
                Message::Data(data) => a.push(data.clone()),
                _ => (),
            };
        }
        return a;
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
        assert_eq!(Message::data_from_iter(iter.iter()), vec![1, 2, 5]);
    }
}
