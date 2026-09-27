use crate::error::Error;
use crate::graph::Pushable;
use crate::message::Message;
use crate::signal::Origin;

/// Push `message` into every sink: clones for all but the last, which takes it by move.
/// A single output never clones.
pub(crate) fn push_each<DataType, SignalType, SinkType>(
    sinks: &mut [Box<SinkType>],
    message: Message<DataType, SignalType>,
) -> Result<(), Error>
where
    DataType: Clone,
    SignalType: Origin + Clone,
    SinkType: Pushable<DataType = DataType, SignalType = SignalType> + ?Sized,
{
    if let Some((last, rest)) = sinks.split_last_mut() {
        for sink in rest {
            sink.push(message.clone())?;
        }
        last.push(message)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Trackable;
    use crate::edge::sync::Receiver;
    use crate::graph::Sink;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts its clones.
    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);

    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Ordering::Relaxed);
            Counted(self.0.clone())
        }
    }

    type Signal = Trackable<&'static str>;

    fn clones_for(outputs: usize) -> usize {
        let clones = Arc::new(AtomicUsize::new(0));
        let receivers: Vec<Receiver<Counted, Signal>> =
            (0..outputs).map(|_| Receiver::new()).collect();
        let mut sinks: Vec<Box<dyn Sink<DataType = Counted, SignalType = Signal> + Send + Sync>> =
            receivers
                .iter()
                .map(|receiver| {
                    Box::new(receiver.sender())
                        as Box<dyn Sink<DataType = Counted, SignalType = Signal> + Send + Sync>
                })
                .collect();

        push_each(&mut sinks, Message::Data(Counted(clones.clone()))).unwrap();

        for receiver in &receivers {
            assert!(matches!(receiver.poll().unwrap(), Some(Message::Data(_))));
        }
        clones.load(Ordering::Relaxed)
    }

    #[test]
    fn clones_all_but_last() {
        assert_eq!(clones_for(0), 0);
        assert_eq!(clones_for(1), 0);
        assert_eq!(clones_for(3), 2);
    }
}
