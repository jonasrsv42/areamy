use crate::error::{Error, ErrorKind};
use crate::graph::marker::Connection;
use crate::graph::{Add, Sink};
use crate::pull::Pullable;
use crate::work::Workable;

/// [`Pulled`] runs a [Pullable] chain as a work node: each [Workable::work] pulls one message
/// and pushes it into every connected child.
///
/// The pull → work boundary is an ordinary connection:
///
/// ```ignore
/// let frontend = root.then(Framer::new()).then(Mel::new());
/// work::Bidi::connect(work::Pulled::of(frontend), &mut model)?;
/// ```
pub struct Pulled<'params, PullableType: Pullable> {
    pullable: PullableType,
    sinks: Vec<
        Box<
            dyn Sink<DataType = PullableType::DataType, SignalType = PullableType::SignalType>
                + Send
                + Sync
                + 'params,
        >,
    >,
}

impl<'params, PullableType: Pullable> Pulled<'params, PullableType> {
    /// Wrap `pullable`; data, signal and thread types come from it.
    pub fn of(pullable: PullableType) -> Self {
        Pulled {
            pullable,
            sinks: Vec::new(),
        }
    }

    fn close_sinks(&mut self) {
        for sink in self.sinks.iter_mut() {
            let _ = sink.close();
        }
    }
}

impl<PullableType: Pullable> Connection for Pulled<'_, PullableType> {}

impl<PullableType> Workable for Pulled<'_, PullableType>
where
    PullableType: Pullable,
    PullableType::DataType: Clone,
    PullableType::SignalType: Clone,
{
    type ThreadId = PullableType::ThreadId;

    fn work(&mut self) -> Result<(), Error> {
        let message = self.pullable.pull().inspect_err(|e| {
            if matches!(e.kind, ErrorKind::Closed) {
                self.close_sinks();
            }
        })?;

        for sink in self.sinks.iter_mut() {
            sink.push(message.clone())?;
        }

        Ok(())
    }
}

impl<'params, PullableType: Pullable>
    Add<
        dyn Sink<DataType = PullableType::DataType, SignalType = PullableType::SignalType>
            + Send
            + Sync
            + 'params,
    > for Pulled<'params, PullableType>
{
    fn add(
        &mut self,
        sink: Box<
            dyn Sink<DataType = PullableType::DataType, SignalType = PullableType::SignalType>
                + Send
                + Sync
                + 'params,
        >,
    ) -> Result<(), Error> {
        self.sinks.push(sink);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::Pushable;
    use crate::node::line::routine::tests::MockLine;
    use crate::pull::WriterBuffer;
    use crate::work::{self, Line, Reader, Writer};
    use crate::{Message, Pullable, Push};

    #[test]
    fn pulled_chain_fans_out_to_bidi_and_push_children() {
        let buffer = WriterBuffer::new();
        let mut writer = Writer::new(&buffer).unwrap();

        let mut pulled = work::Pulled::of(buffer.then(MockLine::new()));
        let other = Line::of(MockLine::new());
        Push::connect(&mut pulled, &other).unwrap();

        let mut line = Line::of(MockLine::new());
        work::Bidi::connect(pulled, &mut line).unwrap();

        let mut reader = Reader::new(line).unwrap();
        let mut other_reader = Reader::new(other).unwrap();

        writer.push(Message::Data(1)).unwrap();

        // 1 → pull MockLine 2 → work MockLine 4, on both branches.
        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(other_reader.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn pulled_close_closes_children() {
        let buffer = WriterBuffer::new();
        let writer = Writer::new(&buffer).unwrap();

        let mut line = Line::of(MockLine::new());
        work::Bidi::connect(work::Pulled::of(buffer), &mut line).unwrap();
        let mut reader = Reader::new(line).unwrap();

        drop(writer);

        assert!(reader.read().is_err());
    }
}
