use crate::error::Error;
use crate::{fatal, marker::Connection, Pullable, ThreadId};
use std::marker::PhantomData;

// For all `Sync` node implementations or root `nosync` we provide this default implementation for
// convenince.
pub struct NoPull<ThreadId, Message> {
    thread_id: PhantomData<ThreadId>,
    message: PhantomData<Message>,
}

impl<ThreadId, Message> Connection for NoPull<ThreadId, Message> {}

// Default empty `Pullable`.
impl<ThreadIdType, MessageType> Pullable for NoPull<ThreadIdType, MessageType>
where
    ThreadIdType: ThreadId,
    MessageType: Send,
{
    type ThreadId = ThreadIdType;
    type Message = MessageType;

    fn pull(&mut self) -> Result<Self::Message, Error> {
        fatal!("Pulling on `NoPull`").into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultThread;

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

    #[test]
    fn pullable_no_pullable_yields_err() {
        let mut no_pull = NoPull {
            thread_id: PhantomData::<DefaultThread>,
            message: PhantomData::<usize>,
        };

        assert!(no_pull.pull().is_err());
        assert!(no_pull.pull().is_err());
    }
}
