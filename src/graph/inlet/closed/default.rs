use crate::error::Error;
use crate::graph::Closed;
use std::cell::RefCell;
use std::rc::Rc;

impl<T: Closed> Closed for Rc<RefCell<T>> {
    fn closed(&self) -> Result<bool, Error> {
        self.borrow().closed()
    }
}

/// Fan-in: closed only when ALL inputs are, as with [crate::graph::Receivable] for `Vec`.
impl<T: Closed> Closed for Vec<T> {
    fn closed(&self) -> Result<bool, Error> {
        for input in self {
            if !input.closed()? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::Closed;
    use crate::poll::edge::{self, Receiver, Sender};
    use crate::signal::Trackable;
    use crate::wake::mock::noop_local_waker;

    type Signal = Trackable<&'static str>;

    fn edge() -> (Sender<usize, Signal>, Receiver<usize, Signal>) {
        edge::channel(noop_local_waker(), None)
    }

    #[test]
    fn fan_in_is_closed_only_once_every_parent_is() {
        let (first, first_rx) = edge();
        let (second, second_rx) = edge();
        let parents = vec![first_rx, second_rx];
        assert!(!parents.closed().unwrap());

        drop(first);
        assert!(!parents.closed().unwrap());

        drop(second);
        assert!(parents.closed().unwrap());
    }
}
