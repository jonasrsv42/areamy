use crate::error::Error;
use crate::message::Message;
use crate::signal::Origin;
use crate::{Closeable, Pushable};

impl<DataType, SignalType> Pushable
    for Box<dyn Closeable<DataType = DataType, SignalType = SignalType>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    type DataType = DataType;
    type SignalType = SignalType;

    fn push(&mut self, object: Message<Self::DataType, Self::SignalType>) -> Result<(), Error> {
        self.as_mut().push(object)
    }
}

impl<DataType, SignalType> Closeable
    for Box<dyn Closeable<DataType = DataType, SignalType = SignalType>>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
{
    fn close(&mut self) -> Result<(), Error> {
        self.as_mut().close()
    }
}

impl<CloseableType, DataType, SignalType> Closeable for Box<CloseableType>
where
    DataType: Send + Sync,
    SignalType: Origin + Send + Sync,
    CloseableType: Closeable<DataType = DataType, SignalType = SignalType>,
{
    fn close(&mut self) -> Result<(), Error> {
        CloseableType::close(self.as_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, SyncEdge};
    use std::sync::Arc;

    fn close(closeable: &mut impl Closeable<DataType = usize, SignalType = usize>) {
        closeable.close().unwrap();
    }

    #[test]
    fn closeable_arc_dyn_can_close() {
        let queue = Arc::new(SyncEdge::<usize, usize>::new());

        let mut closeable: Box<dyn Closeable<DataType = usize, SignalType = usize>> =
            Box::new(queue.clone());

        // Push some data first
        closeable.push(Message::Data(5)).unwrap();

        // Close it
        close(&mut closeable);

        // Can still read buffered data
        assert_eq!(queue.read_front().unwrap(), Message::Data(5));

        // But push should fail now
        let result = queue.push_back(Message::Data(6));
        assert!(result.is_err());
    }

    #[test]
    fn closeable_arc_can_close() {
        let queue = Arc::new(SyncEdge::<usize, usize>::new());
        let mut closeable: Box<Arc<SyncEdge<usize, usize>>> = Box::new(queue.clone());

        closeable.push(Message::Data(5)).unwrap();
        close(&mut closeable);

        assert_eq!(queue.read_front().unwrap(), Message::Data(5));

        let result = queue.push_back(Message::Data(6));
        assert!(result.is_err());
    }
}
