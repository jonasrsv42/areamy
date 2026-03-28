use crate::Closeable;
use crate::error::Error;
use crate::signal::Origin;
use std::cell::RefCell;
use std::rc::Rc;

impl<CloseableType: ?Sized, DataType, SignalType> Closeable for Box<CloseableType>
where
    SignalType: Origin,
    CloseableType: Closeable<DataType = DataType, SignalType = SignalType>,
{
    fn close(&mut self) -> Result<(), Error> {
        CloseableType::close(self.as_mut())
    }
}

impl<T: Closeable> Closeable for Vec<T>
where
    T::DataType: Clone,
    T::SignalType: Clone,
{
    fn close(&mut self) -> Result<(), Error> {
        for edge in self.iter_mut() {
            edge.close()?;
        }
        Ok(())
    }
}

impl<T: Closeable> Closeable for Rc<RefCell<T>> {
    fn close(&mut self) -> Result<(), Error> {
        self.borrow_mut().close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Pushable;
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
