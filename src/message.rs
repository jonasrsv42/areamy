use crate::Origin;
use std::fmt::Debug;

// All data types that flow through our compuation graph
// are `Message`. A `Message` will either hold `Data`
// which can be transformed through computation or
// various signals such as `Flush` or `Marker`
// that will not be transformed but can be
// used as synchronization primitives.
#[derive(Debug, Clone, PartialEq)]
pub enum Message<DataType, SignalType>
where
    DataType: Clone + Sync + Send,
    SignalType: Origin + Sync + Send,
{
    Data(DataType),
    // When a node receives `Flush` it should emit any existing result
    // it has and reset it's computation state.
    Flush(SignalType),
    // A marker should only be passed a long, it can be used for synchronization.
    Marker(SignalType),
}

impl<DataType, SignalType> Message<DataType, SignalType>
where
    DataType: Clone + Sync + Send,
    SignalType: Origin + Sync + Send,
{
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
