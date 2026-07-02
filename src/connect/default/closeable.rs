use crate::Closeable;
use crate::error::Error;
use std::cell::RefCell;
use std::rc::Rc;

impl<T: ?Sized + Closeable> Closeable for Box<T> {
    fn close(&mut self) -> Result<(), Error> {
        T::close(self.as_mut())
    }
}

impl<T: Closeable> Closeable for Vec<T> {
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
    use crate::Message;
    use crate::Pushable;
    use crate::Sink;
    use crate::connect::sync::{Receiver, Sender};

    fn close(closeable: &mut impl Sink<DataType = usize, SignalType = usize>) {
        closeable.close().unwrap();
    }

    #[test]
    fn closeable_boxed_dyn_can_close() {
        let rx = Receiver::<usize, usize>::new();
        let tx = rx.sender();

        let mut closeable: Box<dyn Sink<DataType = usize, SignalType = usize>> = Box::new(tx);

        closeable.push(Message::Data(5)).unwrap();
        close(&mut closeable);

        assert_eq!(rx.read_front().unwrap(), Message::Data(5));

        // Channel is closed; further pushes fail.
        let extra = rx.sender();
        assert!(extra.push_back(Message::Data(6)).is_err());
    }

    #[test]
    fn closeable_boxed_concrete_can_close() {
        let rx = Receiver::<usize, usize>::new();
        let tx = rx.sender();
        let mut closeable: Box<Sender<usize, usize>> = Box::new(tx);

        closeable.push(Message::Data(5)).unwrap();
        close(&mut closeable);

        assert_eq!(rx.read_front().unwrap(), Message::Data(5));

        let extra = rx.sender();
        assert!(extra.push_back(Message::Data(6)).is_err());
    }
}
