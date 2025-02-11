#[cfg(test)]
mod tests {
    use crate::{Connection, DefaultThread, Pullable, error::Error};

    struct Root {
        value: usize,
    }

    impl Connection for Root {}

    impl Pullable for Root {
        type ThreadId = DefaultThread;
        type Message = usize;

        fn pull(&mut self) -> Result<Self::Message, Error> {
            self.value += 1;
            Ok(self.value)
        }
    }

    #[test]
    fn pullable_can_pull() {
        let mut pullable = Root { value: 0 };

        assert_eq!(pullable.pull().unwrap(), 1);
        assert_eq!(pullable.pull().unwrap(), 2);
        assert_eq!(pullable.pull().unwrap(), 3);
    }
}
