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
    use crate::SyncQueue;
    use std::sync::Arc;

    fn push(pushable: &mut impl Pushable<Message = usize>, value: usize) {
        pushable.push(value).unwrap();
    }

    #[test]
    fn pushable_can_push() {
        let mut pushable = SyncQueue::new();
        push(&mut pushable, 5);
        assert_eq!(pushable.read_all().unwrap(), vec![5]);
    }

    #[test]
    fn pushable_arc_dyn_can_push() {
        let queue = Arc::new(SyncQueue::new());

        let mut pushable: Box<dyn Pushable<Message = usize>> = Box::new(queue.clone());
        push(&mut pushable, 5);
        assert_eq!(queue.read_all().unwrap(), vec![5]);
    }

    #[test]
    fn pushable_arc_can_push() {
        let queue = Arc::new(SyncQueue::new());
        let mut pushable: Box<Arc<SyncQueue<usize>>> = Box::new(queue.clone());
        push(&mut pushable, 5);
        assert_eq!(queue.read_all().unwrap(), vec![5]);
    }
}
