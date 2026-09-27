//! [LineRoutine] is the work horse of all Line nodes. It is a frankenstein [std::ops::Coroutine].

use crate::node::routine;

/// [`LineRoutine`] is a flushable subset of [std::ops::Coroutine] accepting a stream of `In` types through
/// [routine::Send::send] and produce a stream of output with [routine::Next::next].
///
/// [std::ops::Coroutine] was not stable at time of development.
/// [LineRoutine] implements a [routine::Send] for a single input and a [routine::Next] for a single
/// output.
///
/// [routine::Send] contract:
///
/// The routine does not have to produce any output on [routine::Send] and is expected to accumulate
/// state until it can yield on [routine::Next].
///
/// After [routine::Send] is invoked [routine::Next] will be invoked
/// until it yields [Option::None]. Then [routine::Send] will be invoked
/// again and so it may repeat.
///
/// The Routine will loop like that, potentially forever.
///
/// [routine::Flush] contract:
///
/// [routine::Flush] signals to the routine that it should output any state it can into
/// subsequent [routine::Next] and then reset all of its internal state for future
/// [routine::Send] invocations.
///
/// <div class="warning"> The routine should never reset its internal output buffer </div>
///
/// It should only reset all other state associated with processing. In other words:
/// a [routine::Flush] call should only ever create, potentially premature, additional output. A
/// [routine::Flush] call should not remove any output.
///
/// [routine::Next] contract
///
/// [routine::Next] yields the next output available from the [LineRoutine]. If no
/// more output can be yielded without additional [routine::Send] it should yield
/// [Option::None].
///
/// [routine::Send] for a [LineRoutine] cannot be invoked again without [routine::Next]
/// having yielded [Option::None].
///
/// [routine::Next] must only yield [Option::None] if it requires additional
/// [routine::Send] to produce more output. The function should be blocking.
///
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a line routine from `{In}` to `{Out}`",
    note = "needs the supertraits of `LineRoutine` and an explicit `impl LineRoutine<{In}, {Out}> for {Self}`"
)]
pub trait LineRoutine<In, Out>:
    Send + routine::Send<In> + routine::Next<Out> + routine::Flush
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
        pub fn new() -> Self {
            MockLine {
                state: 0,
                out: VecDeque::new(),
            }
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

    impl LineRoutine<usize, usize> for MockLine {}

    pub struct AccMockLine {
        num: Vec<usize>,
        out: VecDeque<Vec<usize>>,
    }

    impl AccMockLine {
        pub fn new() -> Self {
            AccMockLine {
                num: Vec::new(),
                out: VecDeque::new(),
            }
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
        pub fn new(wait: usize) -> Self {
            MockWaitLine {
                out: VecDeque::new(),
                release: false,
                wait,
            }
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

    impl LineRoutine<usize, usize> for MockWaitLine {}

    #[test]
    fn line_basic_work() {
        let mut line = MockLine::new();
        line.send(2).unwrap();

        assert_eq!(line.next().unwrap(), Some(4));
    }
    #[test]
    fn line_basic_acc_work() {
        let mut line = AccMockLine::new();
        line.send(2).unwrap();
        assert_eq!(line.next().unwrap(), None);
        line.send(3).unwrap();

        assert_eq!(line.next().unwrap(), Some(vec![2, 3]));
    }

    #[test]
    fn line_basic_wait_work() {
        let mut line = MockWaitLine::new(4);
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
