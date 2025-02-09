use crate::error::Error;
use crate::{Message, Origin};
use crate::{Pushable, Sink, Trackable};
use std::fmt::Debug;

pub struct BiunionReader<LeftPushableType, RightPushableType, SinkType>
where
    LeftPushableType: Pushable,
    RightPushableType: Pushable,
    SinkType: Sink,
{
    pub left: LeftPushableType,
    pub right: RightPushableType,
    pub output: SinkType,
}

impl<LeftPushableType, RightPushableType, SinkType>
    BiunionReader<LeftPushableType, RightPushableType, SinkType>
where
    LeftPushableType: Pushable,
    RightPushableType: Pushable,
    SinkType: Sink,
{
    pub fn new(
        left: LeftPushableType,
        right: RightPushableType,
        output: SinkType,
    ) -> BiunionReader<LeftPushableType, RightPushableType, SinkType> {
        Self {
            left,
            right,
            output,
        }
    }

    pub fn read(&mut self) -> Result<SinkType::Message, Error> {
        self.output.read()
    }

    pub fn left_push(&mut self, msg: LeftPushableType::Message) -> Result<(), Error> {
        self.left.push(msg)?;
        Ok(())
    }

    pub fn right_push(&mut self, msg: RightPushableType::Message) -> Result<(), Error> {
        self.right.push(msg)?;
        Ok(())
    }
}

impl<Left, Right, Out, OriginType, LeftPushableType, RightPushableType, SinkType>
    BiunionReader<LeftPushableType, RightPushableType, SinkType>
where
    Left: Clone + Send + Sync,
    Right: Clone + Debug + Send + Sync,
    Out: Clone + Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    LeftPushableType: Pushable<Message = Message<Left, Trackable<OriginType>>>,
    RightPushableType: Pushable<Message = Message<Right, Trackable<OriginType>>>,
    SinkType: Sink<Message = Message<Out, Trackable<OriginType>>>,
{
    pub fn mark(&mut self, trackable: Trackable<OriginType>) -> Result<Vec<Out>, Error> {
        let mut datas: Vec<Out> = Vec::new();
        loop {
            let object = self.output.read()?;

            match object {
                Message::Data(data) => datas.push(data.clone()),
                Message::Flush(_) => (),
                Message::Marker(trackable_) => {
                    if trackable_ == trackable && trackable.active() == 2 {
                        return Ok(datas);
                    }
                }
            }
        }
    }

    pub fn right_mark(&mut self, origin: OriginType) -> Result<Vec<Out>, Error> {
        let trackable = Trackable::new(origin);
        self.right_push(Message::Marker(trackable.clone()))?;
        self.mark(trackable)
    }

    pub fn left_mark(&mut self, origin: OriginType) -> Result<Vec<Out>, Error> {
        let trackable = Trackable::new(origin);
        self.left_push(Message::Marker(trackable.clone()))?;
        self.mark(trackable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::biunion::routine::tests::MockBiunion;
    use crate::{sync::make_biunion, sync::Sink, sync::Source};

    #[test]
    fn readers_biunion_read() {
        let mut biun = make_biunion(Ok(MockBiunion::new())).unwrap();

        let left_source = Source::new(&biun.input().left).unwrap();
        let right_source = Source::new(&biun.input().right).unwrap();

        let sink = Sink::new(biun.workable(), biun.output()).unwrap();

        let mut reader = BiunionReader::new(left_source, right_source, sink);

        reader.left_push(Message::Data(1)).unwrap();
        reader.right_push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(7));

        reader.left_push(Message::Flush("left".into())).unwrap();
        reader.right_push(Message::Flush("right".into())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Flush("left".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("right".into()));

        reader.left_push(Message::Data(2)).unwrap();
        reader.right_push(Message::Data(1)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(reader.read().unwrap(), Message::Data(4));
    }
}
