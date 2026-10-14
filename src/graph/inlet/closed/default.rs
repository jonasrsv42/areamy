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
    use crate::poll::PollEdge;
    use crate::poll::waker::mock::noop_local_waker;
    use crate::signal::Trackable;
    use std::cell::RefCell;
    use std::rc::Rc;

    type Edge = Rc<RefCell<PollEdge<usize, Trackable<&'static str>>>>;

    fn edge() -> Edge {
        Rc::new(RefCell::new(PollEdge::new(noop_local_waker())))
    }

    #[test]
    fn fan_in_is_closed_only_once_every_parent_is() {
        let parents = vec![edge(), edge()];
        assert!(!parents.closed().unwrap());

        parents[0].borrow_mut().close().unwrap();
        assert!(!parents.closed().unwrap());

        parents[1].borrow_mut().close().unwrap();
        assert!(parents.closed().unwrap());
    }
}
