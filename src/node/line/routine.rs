//! [LineRoutine] is the work horse of all Line nodes. It is a frankenstein [std::ops::Coroutine].

use crate::error::Error;
use std::collections::VecDeque;

/// Signal to the runtime of the [LineRoutine] state.
pub enum Resume {
    /// The [LineRoutine] is done now, pending further input.
    /// the Node should await additional input and invoke [LineRoutine::work].
    ///
    /// Further calls to [LineRoutine::resume] will only yield await, or an [Error].
    Await,

    /// The [LineRoutine] can be [LineRoutine::resume]
    Continue,
}

/// [`LineRoutine`] is a stateful mapping taking a stream off `In` types and producing
/// a stream of `Out` types through its output queue.
pub trait LineRoutine<In, Out>: Send
where
    In: Clone,
    Out: Clone,
{
    /// [LineRoutine::output] returns a mutable reference of the [LineRoutine]s buffered output.
    fn output(&mut self) -> &mut VecDeque<Out>;

    /// [LineRoutine::work] instructs the [LineRoutine]  to work on the next message
    /// and, Optionally, produce some output in the [LineRoutine::output] queue.
    ///
    /// The routine does not have to produce any output and is expected to accumulate
    /// state while being invoked.
    ///
    /// After [LineRoutine::work] is invoked [LineRoutine::resume] will be invoked
    /// until it yields [Resume::Await]. Then [LineRoutine::work] will be invoked
    /// again and so it repeats.
    ///
    /// ```bash
    /// [LineRoutine::work] -> [LineRoutine::resume] (until [Resume::Await]) -> [LineRoutine::work]
    /// ```
    ///
    /// The Routine will loop like that, potentially forever.
    ///
    /// However, **it is not guaranteed** that [LineRoutine::resume] is invoked again after it yields a
    /// [Resume::Continue] as the parent node may choose to invoke [LineRoutine::work]
    /// upon reception of new input. [LineRoutine] thus has to be robust to [LineRoutine::work] being called at any time.
    /// Even during a [Resume::Continue] loop. The [LineRoutine] may choose to just buffer the new
    /// message internally though.
    ///
    ///
    fn work(&mut self, message: In) -> Result<(), Error>;

    /// [LineRoutine::flush] signals to the routine that it should output any state it can into
    /// the [LineRoutine::output] and then reset all of its internal state.
    ///
    /// <div class="warning"> The routine should never reset its output queue </div>
    ///
    /// It should only reset all other state associated with processing.
    fn flush(&mut self) -> Result<(), Error>;

    /// [LineRoutine::resume] is invoked after [LineRoutine::work] to allow a node to quickly emit
    /// something and then be resumed. [LineRoutine::resume] will keep getting invoked until
    ///
    /// 1. [Resume::Await] is invoked, at which point the next call will be to [LineRoutine::work].
    /// 2. New input data is available for the node, at which point [LineRoutine::work] may be
    ///    invoked again.
    fn resume(&mut self) -> Result<Resume, Error> {
        // Per default our `Routine` is not Resumable so we just await.
        Ok(Resume::Await)
    }

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
        fn work(&mut self, object: usize) -> Result<(), Error> {
            self.state += object;
            self.out.push_back(self.state * 2);

            Ok(())
        }

        fn flush(&mut self) -> Result<(), Error> {
            self.state = 0;
            Ok(())
        }

        fn output(&mut self) -> &mut VecDeque<usize> {
            &mut self.out
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
        fn work(&mut self, object: usize) -> Result<(), Error> {
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

        fn output(&mut self) -> &mut VecDeque<Vec<usize>> {
            &mut self.out
        }
    }

    #[test]
    fn line_basic_work() {
        let mut line = MockLine::new().unwrap();
        line.work(2).unwrap();

        assert_eq!(line.out, vec![4]);
    }
    #[test]
    fn line_basic_acc_work() {
        let mut line = AccMockLine::new().unwrap();
        line.work(2).unwrap();
        line.work(3).unwrap();

        assert_eq!(line.out, vec![vec![2, 3]]);
    }
}
