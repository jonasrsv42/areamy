use crate::error::Error;
use crate::{GraphPushSource, Sink, Trackable};
use crate::{Message, Origin};
use std::fmt::Debug;

pub struct BiunionReader<LeftSourceType, RightSourceType, SinkType>
where
    LeftSourceType: GraphPushSource,
    RightSourceType: GraphPushSource,
    SinkType: Sink,
{
    pub left: LeftSourceType,
    pub right: RightSourceType,
    pub output: SinkType,
}

impl<LeftSourceType, RightSourceType, SinkType>
    BiunionReader<LeftSourceType, RightSourceType, SinkType>
where
    LeftSourceType: GraphPushSource,
    RightSourceType: GraphPushSource,
    SinkType: Sink,
{
    pub fn new(
        left: LeftSourceType,
        right: RightSourceType,
        output: SinkType,
    ) -> BiunionReader<LeftSourceType, RightSourceType, SinkType> {
        Self {
            left,
            right,
            output,
        }
    }

    pub fn read(&mut self) -> Result<Message<SinkType::DataType, SinkType::SignalType>, Error> {
        self.output.read()
    }

    pub fn left_push(
        &mut self,
        msg: Message<LeftSourceType::DataType, LeftSourceType::SignalType>,
    ) -> Result<(), Error> {
        self.left.push(msg)?;
        Ok(())
    }

    pub fn right_push(
        &mut self,
        msg: Message<RightSourceType::DataType, RightSourceType::SignalType>,
    ) -> Result<(), Error> {
        self.right.push(msg)?;
        Ok(())
    }

    /// Close both left and right sources.
    pub fn close(&mut self) -> Result<(), Error> {
        self.left.close()?;
        self.right.close()
    }
}

impl<LeftSourceType, RightSourceType, SinkType> Drop
    for BiunionReader<LeftSourceType, RightSourceType, SinkType>
where
    LeftSourceType: GraphPushSource,
    RightSourceType: GraphPushSource,
    SinkType: Sink,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl<Left, Right, Out, OriginType, LeftSourceType, RightSourceType, SinkType>
    BiunionReader<LeftSourceType, RightSourceType, SinkType>
where
    Left: Send + Sync,
    Right: Debug + Send + Sync,
    Out: Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    LeftSourceType: GraphPushSource<DataType = Left, SignalType = Trackable<OriginType>>,
    RightSourceType: GraphPushSource<DataType = Right, SignalType = Trackable<OriginType>>,
    SinkType: Sink<DataType = Out, SignalType = Trackable<OriginType>>,
{
    pub fn mark(&mut self, trackable: Trackable<OriginType>) -> Result<Vec<Out>, Error> {
        let mut datas: Vec<Out> = Vec::new();
        loop {
            let object = self.output.read()?;

            match object {
                Message::Data(data) => datas.push(data),
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
    use crate::biunion;
    use crate::node::biunion::routine::tests::MockBiunion;
    use crate::{work::Sink, work::Source, work::make_biunion};

    #[test]
    fn readers_biunion_read() {
        let biun = make_biunion(Ok(MockBiunion::new())).unwrap();

        let left_source = Source::new::<biunion::Left>(&biun).unwrap();
        let right_source = Source::new::<biunion::Right>(&biun).unwrap();

        let sink = Sink::new(biun).unwrap();

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
