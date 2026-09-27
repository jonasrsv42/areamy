use crate::error::Error;
use crate::message::Message;
use crate::pull::Pullable;

impl<PullableType: Pullable + ?Sized> Pullable for Box<PullableType> {
    type ThreadId = PullableType::ThreadId;
    type DataType = PullableType::DataType;
    type SignalType = PullableType::SignalType;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        self.as_mut().pull()
    }
}

#[cfg(test)]
mod tests {
    use crate::Trackable;
    use crate::node::line::routine::tests::MockLine;
    use crate::{Connection, DefaultThread, Message, Pullable, error::Error};

    struct Root {
        value: usize,
    }

    impl Connection for Root {}

    impl Pullable for Root {
        type ThreadId = DefaultThread;
        type DataType = usize;
        type SignalType = Trackable<&'static str>;

        fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
            self.value += 1;
            Ok(Message::Data(self.value))
        }
    }

    #[test]
    fn pullable_can_pull() {
        let mut pullable = Root { value: 0 };

        assert_eq!(pullable.pull().unwrap(), Message::Data(1));
        assert_eq!(pullable.pull().unwrap(), Message::Data(2));
        assert_eq!(pullable.pull().unwrap(), Message::Data(3));
    }

    #[test]
    fn box_pullable_can_pull() {
        let mut pullable: Box<
            dyn Pullable<
                    ThreadId = DefaultThread,
                    DataType = usize,
                    SignalType = Trackable<&'static str>,
                >,
        > = Box::new(Root { value: 0 });

        assert_eq!(pullable.pull().unwrap(), Message::Data(1));
        assert_eq!(pullable.pull().unwrap(), Message::Data(2));
        assert_eq!(pullable.pull().unwrap(), Message::Data(3));
    }

    #[test]
    fn box_dyn_pullable_chains_with_then() {
        let boxed: Box<
            dyn Pullable<
                    ThreadId = DefaultThread,
                    DataType = usize,
                    SignalType = Trackable<&'static str>,
                >,
        > = Box::new(Root { value: 0 }.then(MockLine::new()));
        let mut chain = boxed.then(MockLine::new());

        // 1 → 2 → 4, then 2 → 6 → 16.
        assert_eq!(chain.pull().unwrap(), Message::Data(4));
        assert_eq!(chain.pull().unwrap(), Message::Data(16));
    }
}
