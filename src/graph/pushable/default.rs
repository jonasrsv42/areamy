use crate::error::Error;
use crate::graph::{Pushable, TryPush};
use crate::message::Message;
use crate::signal::Origin;
use std::cell::RefCell;
use std::rc::Rc;

/// Fan-out: clones for every edge but the last, which takes the message by move. A single edge
/// never clones.
impl<T: Pushable> Pushable for Vec<T>
where
    T::DataType: Clone,
    T::SignalType: Origin + Clone,
{
    type DataType = T::DataType;
    type SignalType = T::SignalType;

    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        if let Some((last, rest)) = self.split_last_mut() {
            for edge in rest {
                edge.push(msg.clone())?;
            }
            last.push(msg)?;
        }
        Ok(())
    }

    fn try_push(
        &mut self,
        _msg: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error> {
        todo!("fan-out becomes a per-edge outlet state machine")
    }
}

impl<T: Pushable> Pushable for Rc<RefCell<T>> {
    type DataType = T::DataType;
    type SignalType = T::SignalType;

    fn push(&mut self, msg: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.borrow_mut().push(msg)
    }

    fn try_push(
        &mut self,
        msg: Message<Self::DataType, Self::SignalType>,
    ) -> Result<TryPush<Message<Self::DataType, Self::SignalType>>, Error> {
        self.borrow_mut().try_push(msg)
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
    use crate::Message;
    use crate::Trackable;
    use crate::edge::sync::{Receiver, Sender};
    use crate::graph::Sink;
    use crate::graph::tests::Bounded;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn push(
        pushable: &mut impl Pushable<DataType = usize, SignalType = Trackable<&'static str>>,
        value: usize,
    ) {
        pushable.push(Message::Data(value)).unwrap();
    }

    /// Pushes 1 then 2 into a bound-1 sink: the second comes back.
    fn fills_at_one(
        pushable: &mut impl Pushable<DataType = usize, SignalType = Trackable<&'static str>>,
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

    /// Counts its clones.
    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);

    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Ordering::Relaxed);
            Counted(self.0.clone())
        }
    }

    type CountedSink = Box<dyn Sink<DataType = Counted, SignalType = Trackable<&'static str>>>;

    fn clones_for(outputs: usize) -> usize {
        let clones = Arc::new(AtomicUsize::new(0));
        let receivers: Vec<Receiver<Counted, Trackable<&'static str>>> =
            (0..outputs).map(|_| Receiver::new()).collect();
        let mut sinks: Vec<CountedSink> = receivers
            .iter()
            .map(|receiver| Box::new(receiver.sender()) as CountedSink)
            .collect();

        Pushable::push(&mut sinks, Message::Data(Counted(clones.clone()))).unwrap();

        for receiver in &receivers {
            assert!(matches!(receiver.poll().unwrap(), Some(Message::Data(_))));
        }
        clones.load(Ordering::Relaxed)
    }

    #[test]
    fn vec_clones_all_but_last() {
        assert_eq!(clones_for(0), 0);
        assert_eq!(clones_for(1), 0);
        assert_eq!(clones_for(3), 2);
    }
}
