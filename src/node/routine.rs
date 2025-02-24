//! Traits for implementing routines.
//!
//! Routines are inspired by [std::ops::Coroutine], as they should be state machines that are a
//! streaming mapping of some set if inputs to some set of outputs.
//!
//! For example usage of the [crate::node::routine] traits please see the tests in this file.
//! or the various routine implementations such as [crate::LineRoutine]

use crate::error::Error;
use crate::marker::{Multiplicity, Unary};

/// [`Send`] trait is used to implement an input for a routine.  
///
/// [Send] is generic over `Message` to allow sending various types of data to the routine.
/// For example a routine might template over a floating point trait.
///
/// [Send] is generic over [Multiplicity] to allow a routine to have multiple inputs.
/// Per default all implementations are [Unary] unless
/// otherwise stated to avoid specifying [Multiplicity] where not necessary.
pub trait Send<Message, MultiplicityType: Multiplicity = Unary> {
    fn send(&mut self, message: Message) -> Result<(), Error>;
}

/// [`Next`] trait is used to implement an output for a routine.  
///
/// [Next] is generic over `Message` to allow yielding various types of data from the routine.
/// For example a routine might template over a floating point trait.
///
/// [Next] is generic over [Multiplicity] to allow a routine to have multiple outputs.
/// Per default all implementations are [Unary] unless
/// otherwise stated to avoid specifying [Multiplicity] where not necessary.
pub trait Next<Message, MultiplicityType: Multiplicity = Unary> {
    fn next(&mut self) -> Result<Option<Message>, Error>;
}

/// [`Flush`] trait is used to implement handling of the graph [crate::Message::Flush] signal for
/// a routine.
///
/// When flushed a routine should prepare to output everything it can on subsequent [Next]
/// invocations and it should clear its state for subsequent [Send] invocations.
pub trait Flush {
    fn flush(&mut self) -> Result<(), Error>;
}

/// [`Name`] trait is used to name routines for logging purposes.
pub trait Name {
    fn name<'a>(&'a self) -> &'a str {
        "unknown"
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// Our simple routine must be able to accept a usize and yield an usize and accept flush.
    trait MockRoutine: Send<usize> + Next<usize> + Flush {}

    pub struct AddOne {
        out: VecDeque<usize>,
    }

    impl MockRoutine for AddOne {}

    impl Next<usize> for AddOne {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.out.pop_front())
        }
    }

    impl Send<usize> for AddOne {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            Ok(self.out.push_back(message + 1))
        }
    }

    impl Flush for AddOne {
        fn flush(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    pub struct Left;
    pub struct Right;

    /// Mark them as order varieties.
    impl Multiplicity for Left {}
    impl Multiplicity for Right {}

    /// More complicated. Here we have a routine with two inputs and two outputs. One could imagine
    /// any arbitrary combination with various types.
    ///
    /// Users may implement these routine traits and then mark that they implement a full routine to
    /// indicate that they would follow the contract of the routine. The contract should be clearly
    /// stated on each routine trait. For example [crate::LineRoutine]
    trait MockCrossRoutine:
        Send<usize, Right> + Send<usize, Left> + Next<usize, Left> + Next<usize, Right> + Flush
    {
    }

    struct CrossAdder {
        left: VecDeque<usize>,
        right: VecDeque<usize>,
    }

    impl MockCrossRoutine for CrossAdder {}

    impl Send<usize, Left> for CrossAdder {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            Ok(self.right.push_back(message + 1))
        }
    }

    impl Send<usize, Right> for CrossAdder {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            Ok(self.left.push_back(message + 1))
        }
    }

    impl Next<usize, Right> for CrossAdder {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.right.pop_front())
        }
    }

    impl Next<usize, Left> for CrossAdder {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.left.pop_front())
        }
    }

    impl Flush for CrossAdder {
        fn flush(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    /// Example generic routine.
    #[test]
    fn add_one_works() {
        let mut routine: Box<dyn MockRoutine> = Box::new(AddOne {
            out: VecDeque::new(),
        });

        routine.send(5).unwrap();

        assert_eq!(routine.next().unwrap(), Some(6));
        assert_eq!(routine.next().unwrap(), None);

        routine.flush().unwrap();

        routine.send(6).unwrap();
        assert_eq!(routine.next().unwrap(), Some(7));
    }

    /// Example routine with two inputs and two outputs.
    #[test]
    fn add_cross_works() {
        let mut routine: Box<dyn MockCrossRoutine> = Box::new(CrossAdder {
            left: VecDeque::new(),
            right: VecDeque::new(),
        });

        Send::<usize, Left>::send(routine.as_mut(), 5).unwrap();

        // We add across so left input is added to right output.
        assert_eq!(Next::<usize, Left>::next(routine.as_mut()).unwrap(), None);
        assert_eq!(
            Next::<usize, Right>::next(routine.as_mut()).unwrap(),
            Some(6)
        );

        Send::<usize, Right>::send(routine.as_mut(), 5).unwrap();

        // vice versa
        assert_eq!(
            Next::<usize, Left>::next(routine.as_mut()).unwrap(),
            Some(6)
        );
        assert_eq!(Next::<usize, Right>::next(routine.as_mut()).unwrap(), None);

        routine.flush().unwrap();
    }
}
