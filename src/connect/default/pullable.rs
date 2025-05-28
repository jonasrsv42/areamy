#[cfg(test)]
mod tests {
    use crate::{error::Error, Connection, DefaultThread, Message, Pullable};

    struct Root {
        value: usize,
    }

    impl Connection for Root {}

    impl Pullable for Root {
        type ThreadId = DefaultThread;
        type DataType = usize;
        type SignalType = usize;

        fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
            self.value += 1;
            Ok(Message::Data(self.value))
        }
    }

    impl<PullableType: Pullable + ?Sized> Pullable for Box<PullableType> {
        type ThreadId = PullableType::ThreadId;
        type DataType = PullableType::DataType;
        type SignalType = PullableType::SignalType;

        fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
            self.as_mut().pull()
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
            dyn Pullable<ThreadId = DefaultThread, DataType = usize, SignalType = usize>,
        > = Box::new(Root { value: 0 });

        assert_eq!(pullable.pull().unwrap(), Message::Data(1));
        assert_eq!(pullable.pull().unwrap(), Message::Data(2));
        assert_eq!(pullable.pull().unwrap(), Message::Data(3));
    }
}
