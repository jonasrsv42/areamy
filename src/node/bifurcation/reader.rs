use crate::error::Error;
use crate::{DefaultThread, Message, Origin, ThreadId, Trackable, Workable, fatal};
use crate::{Pushable, Sink};
use std::fmt::Debug;

pub struct BifurcationReader<SourceType, LeftSinkType, RightSinkType, ThreadIdType = DefaultThread>
where
    SourceType: Pushable,
    LeftSinkType: Sink,
    RightSinkType: Sink,
    ThreadIdType: ThreadId,
{
    pub input: SourceType,
    pub left: LeftSinkType,
    pub right: RightSinkType,
    pub workable: Box<dyn Workable<ThreadId = ThreadIdType>>,
}

impl<SourceType, LeftSinkType, RightSinkType>
    BifurcationReader<SourceType, LeftSinkType, RightSinkType, DefaultThread>
where
    SourceType: Pushable,
    LeftSinkType: Sink,
    RightSinkType: Sink,
{
    pub fn new(
        input: SourceType,
        left: LeftSinkType,
        right: RightSinkType,
        workable: Box<dyn Workable<ThreadId = DefaultThread>>,
    ) -> BifurcationReader<SourceType, LeftSinkType, RightSinkType, DefaultThread> {
        Self {
            input,
            left,
            right,
            workable,
        }
    }
}

impl<SourceType, LeftSinkType, RightSinkType, ThreadIdType>
    BifurcationReader<SourceType, LeftSinkType, RightSinkType, ThreadIdType>
where
    SourceType: Pushable,
    LeftSinkType: Sink,
    RightSinkType: Sink,
    ThreadIdType: ThreadId,
{
    pub fn left_read(
        &mut self,
    ) -> Result<Message<LeftSinkType::DataType, LeftSinkType::SignalType>, Error> {
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
    ) -> Result<Message<RightSinkType::DataType, RightSinkType::SignalType>, Error> {
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
        object: Message<SourceType::DataType, SourceType::SignalType>,
    ) -> Result<(), Error> {
        self.input.push(object)?;
        Ok(())
    }
}

impl<In, Left, Right, OriginType, SourceType, LeftSinkType, RightSinkType, ThreadIdType>
    BifurcationReader<SourceType, LeftSinkType, RightSinkType, ThreadIdType>
where
    In: Send + Sync,
    Left: Debug + Send + Sync + 'static,
    Right: Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    SourceType: Pushable<DataType = In, SignalType = Trackable<OriginType>>,
    LeftSinkType: Sink<DataType = Left, SignalType = Trackable<OriginType>>,
    RightSinkType: Sink<DataType = Right, SignalType = Trackable<OriginType>>,
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
    use crate::{sink::work::tee, work::Source, work::make_bifurcation};

    #[test]
    fn readers_bifurcation_read() {
        // Same as in sync node.
        let mut bifur = make_bifurcation(Ok(MockBifurcation::new())).unwrap();

        let source = Source::new(&bifur).unwrap();

        let left_sink = tee::Sink::new::<bifurcation::Left>(&mut bifur).unwrap();
        let right_sink = tee::Sink::new::<bifurcation::Right>(&mut bifur).unwrap();

        let mut reader = BifurcationReader::new(source, left_sink, right_sink, bifur);

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
