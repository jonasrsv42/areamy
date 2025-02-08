use crate::error::Error;
use crate::{Message, Origin, Trackable};
use crate::{Pushable, Sink};
use std::fmt::Debug;

pub struct BifurcationReader<SourceType, LeftSinkType, RightSinkType>
where
    SourceType: Pushable,
    LeftSinkType: Sink,
    RightSinkType: Sink,
{
    pub input: SourceType,
    pub left: LeftSinkType,
    pub right: RightSinkType,
}

impl<SourceType, LeftSinkType, RightSinkType>
    BifurcationReader<SourceType, LeftSinkType, RightSinkType>
where
    SourceType: Pushable,
    LeftSinkType: Sink,
    RightSinkType: Sink,
{
    pub fn new(
        input: SourceType,
        left: LeftSinkType,
        right: RightSinkType,
    ) -> BifurcationReader<SourceType, LeftSinkType, RightSinkType> {
        Self { input, left, right }
    }

    pub fn left_read(&mut self) -> Result<LeftSinkType::Message, Error> {
        self.left.read()
    }

    pub fn right_read(&mut self) -> Result<RightSinkType::Message, Error> {
        self.right.read()
    }

    pub fn push(&mut self, object: SourceType::Message) -> Result<(), Error> {
        // Left or right does not matter here since it's the same source.
        self.input.push(object)?;
        Ok(())
    }
}

impl<In, Left, Right, OriginType, SourceType, LeftSinkType, RightSinkType>
    BifurcationReader<SourceType, LeftSinkType, RightSinkType>
where
    In: Clone + Send + Sync,
    Left: Clone + Debug + Send + Sync + 'static,
    Right: Clone + Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    SourceType: Pushable<Message = Message<In, Trackable<OriginType>>>,
    LeftSinkType: Sink<Message = Message<Left, Trackable<OriginType>>>,
    RightSinkType: Sink<Message = Message<Right, Trackable<OriginType>>>,
{
    pub fn left_mark(&mut self, origin: OriginType) -> Result<Vec<Left>, Error> {
        let trackable = Trackable::new(origin);
        self.input.push(Message::Marker(trackable.clone()))?;
        let mut datas: Vec<Left> = Vec::new();
        loop {
            let object = self.left.read()?;

            match object {
                Message::Data(data) => datas.push(data.clone()),
                Message::Flush(_) => (),
                Message::Marker(trackable_) => {
                    if trackable_ == trackable && trackable.active() <= 3 {
                        return Ok(datas);
                    }
                }
            }
        }
    }

    pub fn right_mark(&mut self, origin: OriginType) -> Result<Vec<Right>, Error> {
        let trackable = Trackable::new(origin);
        self.input.push(Message::Marker(trackable.clone()))?;
        let mut datas: Vec<Right> = Vec::new();
        loop {
            let object = self.right.read()?;

            match object {
                Message::Data(data) => datas.push(data.clone()),
                Message::Flush(_) => (),
                Message::Marker(trackable_) => {
                    if trackable_ == trackable && trackable.active() <= 3 {
                        return Ok(datas);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::bifurcation::routine::tests::MockBifurcation;
    use crate::{sync::make_bifurcation, sync::Sink, sync::Source};

    #[test]
    fn readers_bifurcation_read() {
        let bifur = make_bifurcation(Ok(MockBifurcation::new())).unwrap();

        let source = Source::new(bifur.input()).unwrap();

        let left_sink = Sink::new(bifur.workable(), &mut bifur.output().left).unwrap();
        let right_sink = Sink::new(bifur.workable(), &mut bifur.output().right).unwrap();

        let mut reader = BifurcationReader::new(source, left_sink, right_sink);

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
