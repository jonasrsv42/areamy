//! [LineRoutine] is the work horse of all Line nodes. It is a frankenstein [std::ops::Coroutine].

use crate::error::Error;

/// [`LineRoutine`] is a flushable subset of [std::ops::Coroutine] accepting a stream of `In` types through
/// [LineRoutine::send] and produce a stream of output with [LineRoutine::next].
///
/// [std::ops::Coroutine] was not stable at time of development.
pub trait LineRoutine<In, Out>: Send
where
    In: Clone,
    Out: Clone,
{
    /// [LineRoutine::send] input data into the [LineRoutine] for it to work on.
    ///
    /// The routine does not have to produce any output and is expected to accumulate
    /// state until it can yield on [LineRoutine::next].
    ///
    /// After [LineRoutine::send] is invoked [LineRoutine::next] will be invoked
    /// until it yields [Option::None]. Then [LineRoutine::send] will be invoked
    /// again and so it may repeat.
    ///
    /// ```bash
    /// [LineRoutine::send] -> [LineRoutine::next] (until [Option::None]) -> [LineRoutine::send]
    /// ```
    ///
    /// The Routine will loop like that, potentially forever.
    fn send(&mut self, message: In) -> Result<(), Error>;

    /// [LineRoutine::flush] signals to the routine that it should output any state it can into
    /// subsequent [LineRoutine::next] and then reset all of its internal state for future
    /// [LineRoutine::send] invocations.
    ///
    /// <div class="warning"> The routine should never reset its internal output buffer </div>
    ///
    /// It should only reset all other state associated with processing. In other words:
    /// a [LineRoutine::flush] call should only ever create, potentially premature, additional output. A [LineRoutine::flush]
    /// call should not remove any output.
    fn flush(&mut self) -> Result<(), Error>;

    /// [LineRoutine::next] yields the next output available from the [LineRoutine]. If no
    /// more output can be yielded without additional [LineRoutine::send] it should yield
    /// [Option::None].
    ///
    /// [LineRoutine::send] for a [LineRoutine] cannot be invoked again without [LineRoutine::next]
    /// having yielded [Option::None].
    ///
    /// [LineRoutine::next] must only yield [Option::None] if it requires additional
    /// [LineRoutine::send] to produce more output. The function should be blocking.
    fn next(&mut self) -> Result<Option<Out>, Error>;

    /// [LineRoutine::name] is used to improve logging.
    fn name(&self) -> &str {
        return "unknown";
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
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

    impl LineRoutine<usize, usize> for MockLine {
        fn send(&mut self, object: usize) -> Result<(), Error> {
            self.state += object;
            self.out.push_back(self.state * 2);

            Ok(())
        }

        fn flush(&mut self) -> Result<(), Error> {
            self.state = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.out.pop_front())
        }
    }

    pub struct AccMockLine {
        num: Vec<usize>,
        out: VecDeque<Vec<usize>>,
    }

    impl AccMockLine {
        pub fn new() -> Result<Self, Error> {
            Ok(AccMockLine {
                num: Vec::new(),
                out: VecDeque::new(),
            })
        }
    }

    impl LineRoutine<usize, Vec<usize>> for AccMockLine {
        fn send(&mut self, object: usize) -> Result<(), Error> {
            self.num.push(object);

            if self.num.len() == 2 {
                self.out.push_back(self.num.clone());
                self.num.clear();
            }

            Ok(())
        }

        fn flush(&mut self) -> Result<(), Error> {
            self.num.clear();
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Vec<usize>>, Error> {
            Ok(self.out.pop_front())
        }
    }

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

    impl LineRoutine<usize, usize> for MockWaitLine {
        fn send(&mut self, object: usize) -> Result<(), Error> {
            self.out.push_back(object);
            self.release = self.out.len() >= self.wait;

            Ok(())
        }

        fn flush(&mut self) -> Result<(), Error> {
            self.release = true;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<usize>, Error> {
            if self.release {
                Ok(self.out.pop_front())
            } else {
                Ok(None)
            }
        }
    }

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
