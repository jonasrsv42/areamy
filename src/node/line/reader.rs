//! [LineReader] is a convenient way to manage a graph input and output in a single place for
//! linear graphs.
use crate::error::Error;
use crate::{Message, Origin, Trackable};
use crate::{Pushable, Sink};
use std::fmt::Debug;

/// [`LineReader`] provides a type that can accept a [Pushable] and [Sink] then
/// expose functions that makes reading and writing intot the graph simpler.
///
/// A [LineReader] is not necessary to read or write to the graph as the
/// nodes themselves can be read from and written to in varios ways.
/// But the line reader combines a graph source with a graph [Sink] in
/// a convenient struct.
pub struct LineReader<PushableType, SinkType>
where
    PushableType: Pushable,
    SinkType: Sink,
{
    pub source: PushableType,
    pub sink: SinkType,
}

impl<PushableType, SinkType> LineReader<PushableType, SinkType>
where
    PushableType: Pushable,
    SinkType: Sink,
{
    /// Create a [LineReader] from a [Pushable] and [Sink]
    pub fn new(source: PushableType, sink: SinkType) -> LineReader<PushableType, SinkType> {
        Self { source, sink }
    }

    /// Read a [Sink::Message] from the line.
    pub fn read(&mut self) -> Result<SinkType::Message, Error> {
        return self.sink.read();
    }

    /// Push a [Pushable::Message] into the line.
    pub fn push(&mut self, object: PushableType::Message) -> Result<(), Error> {
        self.source.push(object)?;

        Ok(())
    }
}

impl<In, Out, OriginType, PushableType, SinkType> LineReader<PushableType, SinkType>
where
    In: Clone + Send + Sync,
    Out: Clone + Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    PushableType: Pushable<Message = Message<In, Trackable<OriginType>>>,
    SinkType: Sink<Message = Message<Out, Trackable<OriginType>>>,
{
    /// Flush and read operation. It will issue a flush which
    /// will empty and reset the line and then read until
    /// the flush is recieved at the `output` end.
    ///
    /// The flush has to be a [Origin] type such that we can track
    /// it while it is traversing through the graph. The tracking is
    /// crucial to ensure that signal handling behaves reasonably in
    /// cyclical and non linear graphs.
    pub fn flush(&mut self, origin: OriginType) -> Result<Vec<Out>, Error> {
        let trackable = Trackable::new(origin);

        self.source.push(Message::Flush(trackable.clone()))?;
        let mut datas: Vec<Out> = Vec::new();
        loop {
            let object = self.sink.read()?;

            match object {
                Message::Data(data) => datas.push(data.clone()),
                Message::Flush(trackable_) => {
                    if trackable_ == trackable && trackable.active() == 2 {
                        return Ok(datas);
                    }
                }
                Message::Marker(_) => (),
            }
        }
    }

    /// Mark and read operation. It will issue a signal which
    /// will be a no-op for the Nodes in the line and then read until
    /// the signal is recieved at the `output` end. This is useful
    /// for synchronizing behaviours and ensuring all output is read.
    ///
    /// The mark has to be a [Origin] type such that we can track
    /// it while it is traversing through the graph. The tracking is
    /// crucial to ensure that signal handling behaves reasonably in
    /// cyclical and non linear graphs.
    pub fn mark(&mut self, origin: OriginType) -> Result<Vec<Out>, Error> {
        let trackable = Trackable::new(origin);
        self.source.push(Message::Marker(trackable.clone()))?;
        let mut datas: Vec<Out> = Vec::new();
        loop {
            let object = self.sink.read()?;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::line::routine::tests::MockLine;
    use crate::{sync::make_line, sync::Sink, sync::Source};

    #[test]
    fn readers_line_objects() {
        let line = make_line(MockLine::new()).unwrap();
        let source = Source::new(&line).unwrap();
        let sink = Sink::new(line).unwrap();

        let mut reader = LineReader::new(source, sink);

        reader.push(Message::Data(1)).unwrap();
        reader.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(6));

        // Reset processing
        reader.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        reader.read().unwrap();

        reader.push(Message::Data(2)).unwrap();
        assert_eq!(reader.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn readers_line_mread() {
        let line = make_line(MockLine::new()).unwrap();
        let source = Source::new(&line).unwrap();
        let sink = Sink::new(line).unwrap();

        let mut reader = LineReader::new(source, sink);

        reader.push(Message::Data(1)).unwrap();
        reader.push(Message::Data(2)).unwrap();

        let res: Vec<usize> = reader.mark("unknown").unwrap();

        assert_eq!(res, vec![2, 6]);
    }
}
