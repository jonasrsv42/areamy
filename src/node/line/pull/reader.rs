use crate::Pullable;
use crate::error::Error;
use crate::message::Message;

/// [`read_until`] will [Pullable::pull] until it recieves the target [Message<DataType, SignalType>]
///
/// * `pullable` - the [Pullable] to [Pullable::pull].
/// * `condition` - the target message to stop at
pub fn read_until<ThreadIdType, DataType, SignalType>(
    pullable: &mut impl Pullable<ThreadId = ThreadIdType, DataType = DataType, SignalType = SignalType>,
    condition: Message<DataType, SignalType>,
) -> Result<Vec<Message<DataType, SignalType>>, Error>
where
    DataType: Send + Sync,
    SignalType: crate::Origin + Send + Sync,
    Message<DataType, SignalType>: PartialEq,
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
    use crate::node::line::pull::builder::make_pull;
    use crate::work::Writer;
    use crate::writer::pull::WriterBuffer;
    use crate::{DefaultThread, Message, Pushable};
    use crate::{Next, Send};
    use std::collections::VecDeque;

    pub struct Identity {
        output: VecDeque<usize>,
    }

    impl Identity {
        pub fn new() -> Self {
            Self {
                output: VecDeque::new(),
            }
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
        let buffer = WriterBuffer::<usize, usize, DefaultThread>::new();
        let mut writer = Writer::of(&buffer).unwrap();

        let mut line = make_pull(buffer, Identity::new());

        writer.push(Message::Data(1)).unwrap();
        writer.push(Message::Data(2)).unwrap();
        writer.push(Message::Data(3)).unwrap();
        writer.push(Message::Data(4)).unwrap();
        writer.push(Message::Marker(0)).unwrap();
        writer.push(Message::Data(5)).unwrap();
        writer.push(Message::Data(6)).unwrap();
        writer.push(Message::Marker(0)).unwrap();

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
