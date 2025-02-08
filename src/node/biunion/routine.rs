use crate::error::Error;
use std::collections::VecDeque;

pub trait BiunionRoutine<Left, Right, Out>: Send
where
    Left: Clone,
    Right: Clone,
    Out: Clone,
{
    // Retrieve the left output queue
    fn output(&mut self) -> &mut VecDeque<Out>;

    // Work on an object, this method performs processing in a synchronous manner.
    fn left_work(&mut self, object: Left) -> Result<(), Error>;

    // Work on an object, this method performs processing in a synchronous manner.
    fn right_work(&mut self, object: Right) -> Result<(), Error>;

    // Flush the bifurcation, outputting available output and resetting internal state.
    fn flush(&mut self) -> Result<(), Error>;

    fn name(&self) -> &str {
        return "unknown";
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::collections::VecDeque;

    pub struct MockBiunion {
        pub shared_state: usize,
        pub output: VecDeque<usize>,
    }

    impl MockBiunion {
        pub fn new() -> Self {
            MockBiunion {
                shared_state: 0,
                output: VecDeque::new(),
            }
        }
    }

    impl BiunionRoutine<usize, usize, usize> for MockBiunion {
        fn output(&mut self) -> &mut VecDeque<usize> {
            &mut self.output
        }

        fn left_work(&mut self, object: usize) -> Result<(), Error> {
            self.output.push_back(object * 2 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }

        fn right_work(&mut self, object: usize) -> Result<(), Error> {
            self.output.push_back(object * 3 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }

        fn flush(&mut self) -> Result<(), Error> {
            self.shared_state = 0;
            Ok(())
        }
    }
}
