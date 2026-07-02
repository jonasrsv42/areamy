use crate::error::Error;
use crate::{Message, Origin};
use crate::{Reader, Sink, Trackable};
use std::fmt::Debug;

pub struct BiunionIo<LeftWriterType, RightWriterType, ReaderType>
where
    LeftWriterType: Sink,
    RightWriterType: Sink,
    ReaderType: Reader,
{
    pub left: LeftWriterType,
    pub right: RightWriterType,
    pub output: ReaderType,
}

impl<LeftWriterType, RightWriterType, ReaderType>
    BiunionIo<LeftWriterType, RightWriterType, ReaderType>
where
    LeftWriterType: Sink,
    RightWriterType: Sink,
    ReaderType: Reader,
{
    pub fn new(
        left: LeftWriterType,
        right: RightWriterType,
        output: ReaderType,
    ) -> BiunionIo<LeftWriterType, RightWriterType, ReaderType> {
        Self {
            left,
            right,
            output,
        }
    }

    pub fn read(&mut self) -> Result<Message<ReaderType::DataType, ReaderType::SignalType>, Error> {
        self.output.read()
    }

    pub fn left_push(
        &mut self,
        msg: Message<LeftWriterType::DataType, LeftWriterType::SignalType>,
    ) -> Result<(), Error> {
        self.left.push(msg)?;
        Ok(())
    }

    pub fn right_push(
        &mut self,
        msg: Message<RightWriterType::DataType, RightWriterType::SignalType>,
    ) -> Result<(), Error> {
        self.right.push(msg)?;
        Ok(())
    }

    /// Close both left and right writers.
    pub fn close(&mut self) -> Result<(), Error> {
        self.left.close()?;
        self.right.close()
    }
}

impl<LeftWriterType, RightWriterType, ReaderType> Drop
    for BiunionIo<LeftWriterType, RightWriterType, ReaderType>
where
    LeftWriterType: Sink,
    RightWriterType: Sink,
    ReaderType: Reader,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl<Left, Right, Out, OriginType, LeftWriterType, RightWriterType, ReaderType>
    BiunionIo<LeftWriterType, RightWriterType, ReaderType>
where
    Left: Send + Sync,
    Right: Debug + Send + Sync,
    Out: Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    LeftWriterType: Sink<DataType = Left, SignalType = Trackable<OriginType>>,
    RightWriterType: Sink<DataType = Right, SignalType = Trackable<OriginType>>,
    ReaderType: Reader<DataType = Out, SignalType = Trackable<OriginType>>,
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
    use crate::{work::Reader, work::Writer, work::make_biunion};

    #[test]
    fn readers_biunion_read() {
        let biun = make_biunion(MockBiunion::new());

        let left_writer = Writer::new::<biunion::Left>(&biun).unwrap();
        let right_writer = Writer::new::<biunion::Right>(&biun).unwrap();

        let output = Reader::new(biun).unwrap();

        let mut reader = BiunionIo::new(left_writer, right_writer, output);

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
