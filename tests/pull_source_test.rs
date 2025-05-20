use areamy::error::Error;
use areamy::fatal;
use areamy::marker::Connection;
use areamy::message::Message;
use areamy::pull::make_pull;
use areamy::pull::Sink;
use areamy::LineRoutine;
use areamy::PullSource;
use areamy::{DefaultThread, Pullable};
use areamy::{Origin, Trackable};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Cursor, Read};

/// Custom signal type for our disk source
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileSignal {
    /// Indicates end of file has been reached
    EndOfFile,
    /// Other signal type for demonstration
    Marker,
}

// Implement the Origin trait for our signal enum
impl Origin for FileSignal {}

/// An example implementation of a reader-based data source that works with in-memory data
/// but demonstrates how PullSource could be used with file I/O and other readable sources.
pub struct MemorySource<R>
where
    R: Read + Send,
{
    reader: BufReader<R>,
    eof: bool,
}

impl<R> Connection for MemorySource<R> where R: Read + Send {}

impl<R> MemorySource<R>
where
    R: Read + Send,
{
    /// Creates a new `MemorySource` from any `Read` implementation.
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            eof: false,
        }
    }

    /// Reads the next line from the reader.
    fn read_line(&mut self) -> Result<Option<String>, Error> {
        if self.eof {
            return Ok(None);
        }

        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => {
                self.eof = true;
                Ok(None)
            }
            Ok(_) => {
                // Remove trailing newline if present
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
                Ok(Some(line))
            }
            Err(e) => fatal!(format!("Failed to read line: {}", e)).into(),
        }
    }
}

impl<R> Pullable for MemorySource<R>
where
    R: Read + Send,
{
    type ThreadId = DefaultThread;
    type DataType = String;
    type SignalType = Trackable<FileSignal>;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        match self.read_line()? {
            Some(line) => Ok(Message::Data(line)),
            None => Ok(Message::Flush(Trackable::new(FileSignal::EndOfFile))),
        }
    }
}

impl<R> PullSource for MemorySource<R> where R: Read + Send {}

// Simple processor that transforms data - in this case, converts text to uppercase
struct TextProcessor {
    queue: VecDeque<String>,
}

impl areamy::Send<String> for TextProcessor {
    fn send(&mut self, data: String) -> Result<(), Error> {
        self.queue.push_back(data.to_uppercase());
        Ok(())
    }
}

impl areamy::Next<String> for TextProcessor {
    fn next(&mut self) -> Result<Option<String>, Error> {
        Ok(self.queue.pop_front())
    }
}

impl areamy::Flush for TextProcessor {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl areamy::node::Name for TextProcessor {
    fn name<'a>(&'a self) -> &'a str {
        "TextProcessor"
    }
}

impl LineRoutine<String, String> for TextProcessor {}

impl TextProcessor {
    fn new() -> Result<Self, Error> {
        Ok(Self {
            queue: VecDeque::new(),
        })
    }
}

#[test]
fn test_memory_source() -> Result<(), Error> {
    // Create a sample in-memory data source
    let test_data = "hello world\ntest data\nthird line";
    let cursor = Cursor::new(test_data);

    // Create our memory-based source implementation
    let memory_source = MemorySource::new(cursor);

    // Create a processing node that converts text to uppercase
    let processor_result = TextProcessor::new();
    let line = make_pull(memory_source, processor_result)?;

    // Create a sink to pull and process data
    let mut sink = Sink::new(line);

    // Pull and verify the transformed data
    assert_eq!(sink.pull()?, Message::Data("HELLO WORLD".to_string()));
    assert_eq!(sink.pull()?, Message::Data("TEST DATA".to_string()));
    assert_eq!(sink.pull()?, Message::Data("THIRD LINE".to_string()));

    // We should get a flush signal with EndOfFile when we reach the end of the data
    match sink.pull()? {
        Message::Flush(signal) => {
            assert_eq!(*signal, FileSignal::EndOfFile, "Expected EndOfFile signal");
        }
        other => panic!("Expected Flush with EndOfFile, got {:?}", other),
    }

    Ok(())
}

#[test]
fn test_empty_source() -> Result<(), Error> {
    // Create an empty source
    let test_data = "";
    let cursor = Cursor::new(test_data);

    // Create our memory-based source implementation
    let memory_source = MemorySource::new(cursor);

    // Create a processing node
    let processor_result = TextProcessor::new();
    let line = make_pull(memory_source, processor_result)?;

    // Create a sink to pull data
    let mut sink = Sink::new(line);

    // Should immediately get a flush signal with EndOfFile since there's no data
    match sink.pull()? {
        Message::Flush(signal) => {
            assert_eq!(*signal, FileSignal::EndOfFile, "Expected EndOfFile signal");
        }
        other => panic!("Expected Flush with EndOfFile, got {:?}", other),
    }

    Ok(())
}
