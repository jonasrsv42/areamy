use crate::error::Error;
use crate::Pushable;

impl<Message> Pushable for Box<dyn Pushable<Message = Message>>
where
    Message: Clone,
{
    type Message = Message;

    fn push(&mut self, object: Self::Message) -> Result<(), Error> {
        self.as_mut().push(object)
    }
}

impl<PushableType, MessageType> Pushable for Box<PushableType>
where
    MessageType: Clone + Send + Sync,
    PushableType: Pushable<Message = MessageType>,
{
    fn push(&mut self, object: Self::Message) -> Result<(), Error> {
        PushableType::push(self.as_mut(), object)
    }

    type Message = MessageType;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, SyncEdge};
    use std::sync::Arc;

    fn push(pushable: &mut impl Pushable<Message = Message<usize, usize>>, value: usize) {
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

        let mut pushable: Box<dyn Pushable<Message = Message<usize, usize>>> = Box::new(queue.clone());
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
