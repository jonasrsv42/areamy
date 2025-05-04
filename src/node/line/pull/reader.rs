use crate::error::Error;
use crate::Pullable;

/// [`read_until`] will [Pullable::pull] until it recieves the target [Pullable::Message]
///
/// * `pullable` - the [Pullable] to [Pullable::pull].
/// * `condition` - a [Pullable::Message]
pub fn read_until<ThreadIdType, MessageType>(
    pullable: &mut impl Pullable<ThreadId = ThreadIdType, Message = MessageType>,
    condition: MessageType,
) -> Result<Vec<MessageType>, Error>
where
    MessageType: PartialEq,
{
    let mut a = Vec::new();
    loop {
        let message = pullable.pull()?;

        if message == condition {
            return Ok(a);
        }

        a.push(message)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::line::pull::builder::{make_pull, Root};
    use crate::work::Source;
    use crate::{DefaultThread, Message, Pushable};
    use crate::{Next, Send};
    use std::collections::VecDeque;

    pub struct Identity {
        output: VecDeque<usize>,
    }

    impl Identity {
        pub fn new() -> Result<Self, Error> {
            Ok(Self {
                output: VecDeque::new(),
            })
        }
    }

    impl Send<usize> for Identity {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            Ok(self.output.push_back(message))
        }
    }

    impl Next<usize> for Identity {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.output.pop_front())
        }
    }

    impl crate::Flush for Identity {
        fn flush(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    impl crate::node::Name for Identity {}
    impl crate::LineRoutine<usize, usize> for Identity {}

    #[test]
    fn line_nosync_readers_basic() {
        let root = Root::<usize, usize, DefaultThread>::new();
        let mut source = Source::of(&root).unwrap();

        let mut line = make_pull(root, Identity::new()).unwrap();

        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();
        source.push(Message::Data(3)).unwrap();
        source.push(Message::Data(4)).unwrap();
        source.push(Message::Marker(0)).unwrap();
        source.push(Message::Data(5)).unwrap();
        source.push(Message::Data(6)).unwrap();
        source.push(Message::Marker(0)).unwrap();

        assert_eq!(
            read_until(&mut line, Message::Marker(0)).unwrap(),
            vec![
                Message::Data(1),
                Message::Data(2),
                Message::Data(3),
                Message::Data(4)
            ]
        );

        assert_eq!(
            read_until(&mut line, Message::Marker(0)).unwrap(),
            vec![Message::Data(5), Message::Data(6),]
        );
    }
}
