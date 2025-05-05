#[cfg(test)]
mod tests {
    use crate::{Connection, DefaultThread, Message, Pullable, error::Error};

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

    #[test]
    fn pullable_can_pull() {
        let mut pullable = Root { value: 0 };

        assert_eq!(pullable.pull().unwrap(), Message::Data(1));
        assert_eq!(pullable.pull().unwrap(), Message::Data(2));
        assert_eq!(pullable.pull().unwrap(), Message::Data(3));
    }
}
