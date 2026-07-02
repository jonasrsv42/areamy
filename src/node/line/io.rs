//! [LineIo] is a convenient way to manage a graph input and output in a single place for
//! linear graphs.
use crate::error::Error;
use crate::{Message, Origin, Trackable};
use crate::{Reader, Sink};
use std::fmt::Debug;

/// [`LineIo`] provides a type that can accept a [Writer](crate::work::Writer) and [Reader] then
/// expose functions that makes reading and writing into the graph simpler.
///
/// A [LineIo] is not necessary to read or write to the graph as the
/// nodes themselves can be read from and written to in various ways.
/// But the [LineIo] combines a graph [Writer](crate::work::Writer) with a graph [Reader] in
/// a convenient struct.
pub struct LineIo<WriterType, ReaderType>
where
    WriterType: Sink,
    ReaderType: Reader,
{
    pub writer: WriterType,
    pub reader: ReaderType,
}

impl<WriterType, ReaderType> LineIo<WriterType, ReaderType>
where
    WriterType: Sink,
    ReaderType: Reader,
{
    /// Create a [LineIo] from a [Writer](crate::work::Writer) and [Reader]
    pub fn new(writer: WriterType, reader: ReaderType) -> LineIo<WriterType, ReaderType> {
        Self { writer, reader }
    }

    /// Read a Message from the line.
    pub fn read(&mut self) -> Result<Message<ReaderType::DataType, ReaderType::SignalType>, Error> {
        return self.reader.read();
    }

    /// Push a Message into the line.
    pub fn push(
        &mut self,
        object: Message<WriterType::DataType, WriterType::SignalType>,
    ) -> Result<(), Error> {
        self.writer.push(object)?;

        Ok(())
    }

    /// Close the writer, signaling no more data will be produced.
    /// This will cause downstream readers to receive [crate::error::ErrorKind::Closed]
    /// when the buffer is empty.
    pub fn close(&mut self) -> Result<(), Error> {
        self.writer.close()
    }
}

impl<WriterType, ReaderType> Drop for LineIo<WriterType, ReaderType>
where
    WriterType: Sink,
    ReaderType: Reader,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl<In, Out, OriginType, WriterType, ReaderType> LineIo<WriterType, ReaderType>
where
    In: Send + Sync,
    Out: Debug + Send + Sync + 'static,
    OriginType: Origin + Clone + Send + Sync + 'static,
    WriterType: Sink<DataType = In, SignalType = Trackable<OriginType>>,
    ReaderType: Reader<DataType = Out, SignalType = Trackable<OriginType>>,
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

        self.writer.push(Message::Flush(trackable.clone()))?;
        let mut datas: Vec<Out> = Vec::new();
        loop {
            let object = self.reader.read()?;

            match object {
                Message::Data(data) => datas.push(data),
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
        self.writer.push(Message::Marker(trackable.clone()))?;
        let mut datas: Vec<Out> = Vec::new();
        loop {
            let object = self.reader.read()?;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::line::routine::tests::MockLine;
    use crate::{work::Reader, work::Writer, work::make_line};

    #[test]
    fn readers_line_objects() {
        let line = make_line(MockLine::new());
        let writer = Writer::new(&line).unwrap();
        let reader = Reader::new(line).unwrap();

        let mut io = LineIo::new(writer, reader);

        io.push(Message::Data(1)).unwrap();
        io.push(Message::Data(2)).unwrap();

        assert_eq!(io.read().unwrap(), Message::Data(2));
        assert_eq!(io.read().unwrap(), Message::Data(6));

        // Reset processing
        io.push(Message::Flush("hi".into())).unwrap();
        // Read the Flush
        io.read().unwrap();

        io.push(Message::Data(2)).unwrap();
        assert_eq!(io.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn readers_line_mread() {
        let line = make_line(MockLine::new());
        let writer = Writer::new(&line).unwrap();
        let reader = Reader::new(line).unwrap();

        let mut io = LineIo::new(writer, reader);

        io.push(Message::Data(1)).unwrap();
        io.push(Message::Data(2)).unwrap();

        let res: Vec<usize> = io.mark("unknown").unwrap();

        assert_eq!(res, vec![2, 6]);
    }
}
