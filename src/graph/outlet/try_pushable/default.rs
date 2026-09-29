use crate::error::Error;
use crate::graph::{TryPush, TryPushable};
use crate::message::Message;
use crate::signal::Origin;
use std::cell::RefCell;
use std::rc::Rc;

impl<T: TryPushable> TryPushable for Rc<RefCell<T>> {
    fn try_push(
        &mut self,
        msg: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error> {
        self.borrow_mut().try_push(msg)
    }
}

impl<PushableType: ?Sized, DataType, SignalType> TryPushable for Box<PushableType>
where
    SignalType: Origin,
    PushableType: TryPushable<DataType = DataType, SignalType = SignalType>,
{
    fn try_push(
        &mut self,
        object: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error> {
        PushableType::try_push(self.as_mut(), object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Trackable;
    use crate::graph::tests::Bounded;
    use crate::work::Sink;

    /// Pushes 1 then 2 into a bound-1 sink: the second comes back.
    fn fills_at_one(
        pushable: &mut impl TryPushable<DataType = usize, SignalType = Trackable<&'static str>>,
    ) {
        assert_eq!(
            pushable.try_push(Message::Data(1)).unwrap(),
            TryPush::Pushed
        );
        assert_eq!(
            pushable.try_push(Message::Data(2)).unwrap(),
            TryPush::Full(Message::Data(2))
        );
    }

    #[test]
    fn try_push_forwards_through_box_dyn() {
        let mut pushable: Box<dyn Sink<DataType = usize, SignalType = Trackable<&'static str>>> =
            Box::new(Bounded::new(1));
        fills_at_one(&mut pushable);
    }

    #[test]
    fn try_push_forwards_through_rc_refcell() {
        let bounded = Rc::new(RefCell::new(Bounded::new(1)));
        let mut handle = bounded.clone();
        fills_at_one(&mut handle);
        assert_eq!(bounded.borrow().items, vec![Message::Data(1)]);
    }
}
