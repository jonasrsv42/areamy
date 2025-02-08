use crate::error::Error;
use std::collections::VecDeque;

pub enum Resume {
    // The Routine is for now. It should await
    // the next message to become available.
    Await,

    // The Routine is still Resumable.
    Continue,
}

// A line `Routine` is a stateful mapping.
// Taking a stream on `In` types and produces
// a stream of `Out` types.
pub trait LineRoutine<In, Out>: Send
where
    In: Clone,
    Out: Clone,
{
    // Retrieve the output queue
    fn output(&mut self) -> &mut VecDeque<Out>;

    // `work` is invoked as soon as a `message` is ready. This method performs processing in
    // a synchronous manner. It is expected to populate the output once the routine has
    // accumulated sufficient state.
    fn work(&mut self, message: In) -> Result<(), Error>;

    // Flush the routine. This is expected to output if there's sufficient state and
    // to reset all the internal state of the `Routine`.
    fn flush(&mut self) -> Result<(), Error>;

    // `resume` is invoked after `work` if a Node does not have more messages
    // to work on yet. It will keep getting invoked until
    // 1. `Await` is returned
    // 2. A message becomes available to `work` on.
    // 3. Flush is invoked.
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

    #[test]
    fn line_basic_work() {
        let mut line = MockLine::new().unwrap();
        line.work(2).unwrap();

        assert_eq!(line.out, vec![4]);
    }
}
