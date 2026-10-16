use crate::error::Error;
use crate::graph::Pushable;
use crate::message::Message;
use crate::signal::Origin;

impl<PushableType: ?Sized, DataType, SignalType> Pushable for Box<PushableType>
where
    SignalType: Origin,
    PushableType: Pushable<DataType = DataType, SignalType = SignalType>,
{
    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        PushableType::push(self.as_mut(), object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Trackable;
    use crate::edge::sync::{Receiver, Sender};

    fn push(
        pushable: &mut impl Pushable<DataType = usize, SignalType = Trackable<&'static str>>,
        value: usize,
    ) {
        pushable.push(Message::Data(value)).unwrap();
    }

    #[test]
    fn pushable_sender_can_push() {
        let rx = Receiver::<usize, Trackable<&'static str>>::new();
        let mut tx = rx.sender();
        push(&mut tx, 5);
        assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
    }

    #[test]
    fn pushable_boxed_dyn_can_push() {
        let rx = Receiver::<usize, Trackable<&'static str>>::new();
        let mut pushable: Box<
            dyn Pushable<DataType = usize, SignalType = Trackable<&'static str>>,
        > = Box::new(rx.sender());
        push(&mut pushable, 5);
        assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
    }

    #[test]
    fn pushable_boxed_concrete_can_push() {
        let rx = Receiver::<usize, Trackable<&'static str>>::new();
        let mut pushable: Box<Sender<usize, Trackable<&'static str>>> = Box::new(rx.sender());
        push(&mut pushable, 5);
        assert_eq!(rx.read_all().unwrap(), vec![Message::Data(5)]);
    }
}
