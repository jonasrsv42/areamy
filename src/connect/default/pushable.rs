use crate::Pushable;
use crate::error::Error;
use crate::message::Message;
use crate::signal::Origin;
use std::cell::RefCell;
use std::rc::Rc;

impl<T: Pushable> Pushable for Vec<T>
where
    T::DataType: Clone,
    T::SignalType: Origin + Clone,
{
    type DataType = T::DataType;
    type SignalType = T::SignalType;

    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        for edge in self.iter_mut() {
            edge.push(msg.clone())?;
        }
        Ok(())
    }
}

impl<T: Pushable> Pushable for Rc<RefCell<T>> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;

    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.borrow_mut().push(msg)
    }
}

impl<PushableType: ?Sized, DataType, SignalType> Pushable for Box<PushableType>
where
    SignalType: Origin,
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
    use crate::Message;
    use crate::connect::sync::{Receiver, Sender};

    fn push(pushable: &mut impl Pushable<DataType = usize, SignalType = usize>, value: usize) {
        pushable.push(Message::Data(value)).unwrap();
    }

    #[test]
    fn pushable_sender_can_push() {
        let rx = Receiver::<usize, usize>::new();
        let mut tx = rx.sender();
        push(&mut tx, 5);
        assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
    }

    #[test]
    fn pushable_boxed_dyn_can_push() {
        let rx = Receiver::<usize, usize>::new();
        let mut pushable: Box<dyn Pushable<DataType = usize, SignalType = usize>> =
            Box::new(rx.sender());
        push(&mut pushable, 5);
        assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
    }

    #[test]
    fn pushable_boxed_concrete_can_push() {
        let rx = Receiver::<usize, usize>::new();
        let mut pushable: Box<Sender<usize, usize>> = Box::new(rx.sender());
        push(&mut pushable, 5);
        assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
    }
}
