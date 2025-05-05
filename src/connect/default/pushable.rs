use crate::error::Error;
use crate::message::Message;
use crate::signal::Origin;
use crate::Pushable;

impl<DataType, SignalType> Pushable for Box<dyn Pushable<DataType = DataType, SignalType = SignalType>>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.as_mut().push(object)
    }
}

impl<PushableType, DataType, SignalType> Pushable for Box<PushableType>
where
    DataType: Clone + Send + Sync,
    SignalType: Origin + Send + Sync,
    PushableType: Pushable<DataType = DataType, SignalType = SignalType>,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        PushableType::push(self.as_mut(), object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, SyncEdge};
    use std::sync::Arc;

    fn push(pushable: &mut impl Pushable<DataType = usize, SignalType = usize>, value: usize) {
        pushable.push(Message::Data(value)).unwrap();
    }

    #[test]
    fn pushable_can_push() {
        let mut pushable = SyncEdge::<usize, usize>::new();
        push(&mut pushable, 5);
        assert_eq!(pushable.read_all().unwrap(), vec![Message::Data(5)]);
    }

    #[test]
    fn pushable_arc_dyn_can_push() {
        let queue = Arc::new(SyncEdge::<usize, usize>::new());

        let mut pushable: Box<dyn Pushable<DataType = usize, SignalType = usize>> = Box::new(queue.clone());
        push(&mut pushable, 5);
        assert_eq!(queue.read_all().unwrap(), vec![Message::Data(5)]);
    }

    #[test]
    fn pushable_arc_can_push() {
        let queue = Arc::new(SyncEdge::<usize, usize>::new());
        let mut pushable: Box<Arc<SyncEdge<usize, usize>>> = Box::new(queue.clone());
        push(&mut pushable, 5);
        assert_eq!(queue.read_all().unwrap(), vec![Message::Data(5)]);
    }
}
