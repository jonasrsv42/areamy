//! [LineRoutine] is the work horse of all Line nodes. It is a frankenstein [std::ops::Coroutine].

/// [`LineRoutine`] is a flushable subset of [std::ops::Coroutine] accepting a stream of `In` types through
/// [LineRoutine::send] and produce a stream of output with [LineRoutine::next].
///
/// [std::ops::Coroutine] was not stable at time of development.

/// [LineRoutine] implements a [crate::Send] for a single input and a [crate::Next] for a single
/// output.
///
/// [crate::Send] contract:
///
/// The routine does not have to produce any output on [crate::Send] and is expected to accumulate
/// state until it can yield on [crate::Next].
///
/// After [crate::Send] is invoked [crate::Next] will be invoked
/// until it yields [Option::None]. Then [crate::Send] will be invoked
/// again and so it may repeat.
///
/// The Routine will loop like that, potentially forever.
///
/// [crate::Flush] contract:
///
/// [crate::Flush] signals to the routine that it should output any state it can into
/// subsequent [crate::Next] and then reset all of its internal state for future
/// [crate::Send] invocations.
///
/// <div class="warning"> The routine should never reset its internal output buffer </div>
///
/// It should only reset all other state associated with processing. In other words:
/// a [crate::Flush] call should only ever create, potentially premature, additional output. A
/// [crate::Flush] call should not remove any output.
///
/// [crate::Next] contract
///
/// [crate::Next] yields the next output available from the [LineRoutine]. If no
/// more output can be yielded without additional [crate::Send] it should yield
/// [Option::None].
///
/// [crate::Send] for a [LineRoutine] cannot be invoked again without [crate::Next]
/// having yielded [Option::None].
///
/// [crate::Next] must only yield [Option::None] if it requires additional
/// [crate::Send] to produce more output. The function should be blocking.
///
pub trait LineRoutine<In, Out>:
    Send + crate::Send<In> + crate::Next<Out> + crate::Flush + crate::node::Name
{
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::error::Error;
    use crate::{Next, Send};
    use std::collections::VecDeque;

    pub struct MockLine {
        state: usize,
        out: VecDeque<usize>,
    }

    impl MockLine {
        pub fn new() -> Result<Self, Error> {
            Ok(MockLine {
                state: 0,
                out: VecDeque::new(),
            })
        }
    }

    impl crate::Send<usize> for MockLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.state += message;
            self.out.push_back(self.state * 2);

            Ok(())
        }
    }

    impl crate::Next<usize> for MockLine {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.out.pop_front())
        }
    }

    impl crate::Flush for MockLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.state = 0;
            Ok(())
        }
    }

    impl crate::node::Name for MockLine {}

    impl LineRoutine<usize, usize> for MockLine {}

    pub struct AccMockLine {
        num: Vec<usize>,
        out: VecDeque<Vec<usize>>,
    }

    impl crate::node::Name for AccMockLine {}

    impl AccMockLine {
        pub fn new() -> Result<Self, Error> {
            Ok(AccMockLine {
                num: Vec::new(),
                out: VecDeque::new(),
            })
        }
    }

    impl crate::Send<usize> for AccMockLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.num.push(message);

            if self.num.len() == 2 {
                self.out.push_back(self.num.clone());
                self.num.clear();
            }

            Ok(())
        }
    }

    impl crate::Next<Vec<usize>> for AccMockLine {
        fn next(&mut self) -> Result<Option<Vec<usize>>, Error> {
            Ok(self.out.pop_front())
        }
    }

    impl crate::Flush for AccMockLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.num.clear();
            Ok(())
        }
    }

    impl LineRoutine<usize, Vec<usize>> for AccMockLine {}

    pub struct MockWaitLine {
        out: VecDeque<usize>,

        /// [MockWaitLine::release] indicats that we can release all output.
        release: bool,

        /// Wait for [MockWaitLine::wait] before release is true.
        wait: usize,
    }

    impl MockWaitLine {
        pub fn new(wait: usize) -> Result<Self, Error> {
            Ok(MockWaitLine {
                out: VecDeque::new(),
                release: false,
                wait,
            })
        }
    }

    impl crate::Send<usize> for MockWaitLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.out.push_back(message);
            self.release = self.out.len() >= self.wait;

            Ok(())
        }
    }

    impl crate::Next<usize> for MockWaitLine {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            if self.release {
                Ok(self.out.pop_front())
            } else {
                Ok(None)
            }
        }
    }

    impl crate::Flush for MockWaitLine {
        fn flush(&mut self) -> Result<(), Error> {
            self.release = true;
            Ok(())
        }
    }

    impl crate::node::Name for MockWaitLine {}
    impl LineRoutine<usize, usize> for MockWaitLine {}

    #[test]
    fn line_basic_work() {
        let mut line = MockLine::new().unwrap();
        line.send(2).unwrap();

        assert_eq!(line.next().unwrap(), Some(4));
    }
    #[test]
    fn line_basic_acc_work() {
        let mut line = AccMockLine::new().unwrap();
        line.send(2).unwrap();
        assert_eq!(line.next().unwrap(), None);
        line.send(3).unwrap();

        assert_eq!(line.next().unwrap(), Some(vec![2, 3]));
    }

    #[test]
    fn line_basic_wait_work() {
        let mut line = MockWaitLine::new(4).unwrap();
        line.send(2).unwrap();
        line.send(3).unwrap();
        line.send(4).unwrap();

        assert_eq!(line.next().unwrap(), None);
        line.send(5).unwrap();
        assert_eq!(line.next().unwrap(), Some(2));
        assert_eq!(line.next().unwrap(), Some(3));
        assert_eq!(line.next().unwrap(), Some(4));
        assert_eq!(line.next().unwrap(), Some(5));
    }
}
