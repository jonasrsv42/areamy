use crate::Reader;
use crate::error::Error;
use crate::{DefaultThread, Message, Origin, Sink, ThreadId, Trackable, Workable, fatal};
use std::fmt::Debug;

pub struct BifurcationIo<
    'params,
    WriterType,
    LeftReaderType,
    RightReaderType,
    ThreadIdType = DefaultThread,
> where
    WriterType: Sink,
    LeftReaderType: Reader,
    RightReaderType: Reader,
    ThreadIdType: ThreadId,
{
    pub input: WriterType,
    pub left: LeftReaderType,
    pub right: RightReaderType,
    pub workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
}

impl<'params, WriterType, LeftReaderType, RightReaderType>
    BifurcationIo<'params, WriterType, LeftReaderType, RightReaderType, DefaultThread>
where
    WriterType: Sink,
    LeftReaderType: Reader,
    RightReaderType: Reader,
{
    pub fn new(
        input: WriterType,
        left: LeftReaderType,
        right: RightReaderType,
        workable: Box<dyn Workable<ThreadId = DefaultThread> + 'params>,
    ) -> BifurcationIo<'params, WriterType, LeftReaderType, RightReaderType, DefaultThread> {
        Self {
            input,
            left,
            right,
            workable,
        }
    }
}

impl<'params, WriterType, LeftReaderType, RightReaderType, ThreadIdType>
    BifurcationIo<'params, WriterType, LeftReaderType, RightReaderType, ThreadIdType>
where
    WriterType: Sink,
    LeftReaderType: Reader,
    RightReaderType: Reader,
    ThreadIdType: ThreadId,
{
    pub fn left_read(
        &mut self,
    ) -> Result<Message<LeftReaderType::DataType, LeftReaderType::SignalType>, Error> {
        match self.left.poll()? {
            Some(message) => Ok(message),
            None => {
                self.workable.work()?;
                match self.left.poll()? {
                    Some(message) => Ok(message),
                    None => fatal!("Work did not yield new message.").into(),
                }
            }
        }
    }

    pub fn right_read(
        &mut self,
    ) -> Result<Message<RightReaderType::DataType, RightReaderType::SignalType>, Error> {
        match self.right.poll()? {
            Some(message) => Ok(message),
            None => {
                self.workable.work()?;
                match self.right.poll()? {
                    Some(message) => Ok(message),
                    None => fatal!("Work did not yield new message.").into(),
                }
            }
        }
    }

    pub fn push(
        &mut self,
        object: Message<WriterType::DataType, WriterType::SignalType>,
    ) -> Result<(), Error> {
        self.input.push(object)?;
        Ok(())
    }

    /// Close the input writer.
    pub fn close(&mut self) -> Result<(), Error> {
        self.input.close()
    }
}

impl<'params, WriterType, LeftReaderType, RightReaderType, ThreadIdType> Drop
    for BifurcationIo<'params, WriterType, LeftReaderType, RightReaderType, ThreadIdType>
where
    WriterType: Sink,
    LeftReaderType: Reader,
    RightReaderType: Reader,
    ThreadIdType: ThreadId,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl<
    'params,
    In,
    Left,
    Right,
    OriginType,
    WriterType,
    LeftReaderType,
    RightReaderType,
    ThreadIdType,
> BifurcationIo<'params, WriterType, LeftReaderType, RightReaderType, ThreadIdType>
where
    In: Send + Sync,
    Left: Debug + Send + Sync + 'static,
    Right: Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    WriterType: Sink<DataType = In, SignalType = Trackable<OriginType>>,
    LeftReaderType: Reader<DataType = Left, SignalType = Trackable<OriginType>>,
    RightReaderType: Reader<DataType = Right, SignalType = Trackable<OriginType>>,
    ThreadIdType: ThreadId,
{
    pub fn left_mark(&mut self, origin: OriginType) -> Result<Vec<Left>, Error> {
        let trackable = Trackable::new(origin);
        self.input.push(Message::Marker(trackable.clone()))?;
        let mut datas: Vec<Left> = Vec::new();
        loop {
            match self.left.poll()? {
                Some(message) => match message {
                    Message::Data(data) => datas.push(data),
                    Message::Flush(_) => (),
                    Message::Marker(trackable_) => {
                        if trackable_ == trackable && trackable.active() <= 3 {
                            return Ok(datas);
                        }
                    }
                },
                None => self.workable.work()?,
            }
        }
    }

    pub fn right_mark(&mut self, origin: OriginType) -> Result<Vec<Right>, Error> {
        let trackable = Trackable::new(origin);
        self.input.push(Message::Marker(trackable.clone()))?;
        let mut datas: Vec<Right> = Vec::new();
        loop {
            match self.right.poll()? {
                Some(message) => match message {
                    Message::Data(data) => datas.push(data),
                    Message::Flush(_) => (),
                    Message::Marker(trackable_) => {
                        if trackable_ == trackable && trackable.active() <= 3 {
                            return Ok(datas);
                        }
                    }
                },
                None => self.workable.work()?,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::bifurcation;
    use crate::node::bifurcation::routine::tests::MockBifurcation;
    use crate::{reader::work::tee, work::Writer, work::make_bifurcation};

    #[test]
    fn readers_bifurcation_read() {
        // Same as in sync node.
        let mut bifur = make_bifurcation(MockBifurcation::new());

        let writer = Writer::new(&bifur).unwrap();

        let left_reader = tee::Reader::new::<bifurcation::Left>(&mut bifur).unwrap();
        let right_reader = tee::Reader::new::<bifurcation::Right>(&mut bifur).unwrap();

        let mut reader = BifurcationIo::new(writer, left_reader, right_reader, bifur);

        // Add one flush
        reader.push(Message::Data(1)).unwrap();
        reader.push(Message::Data(2)).unwrap();

        assert_eq!(reader.left_read().unwrap(), Message::Data(2));
        assert_eq!(reader.left_read().unwrap(), Message::Data(5));

        reader.push(Message::Flush("hi".into())).unwrap();

        assert_eq!(reader.right_read().unwrap(), Message::Data(3));
        assert_eq!(reader.right_read().unwrap(), Message::Data(7));

        assert_eq!(reader.right_read().unwrap(), Message::Flush("hi".into()));
        assert_eq!(reader.left_read().unwrap(), Message::Flush("hi".into()));

        reader.push(Message::Data(2)).unwrap();

        assert_eq!(reader.left_read().unwrap(), Message::Data(4));
        assert_eq!(reader.right_read().unwrap(), Message::Data(6));
    }
}
