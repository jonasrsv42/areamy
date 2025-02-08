use crate::error::Error;
use std::collections::VecDeque;

pub trait BifurcationRoutine<In, Left, Right>: Send
where
    In: Clone,
    Left: Clone,
    Right: Clone,
{
    // Retrieve the left output queue
    fn left_output(&mut self) -> &mut VecDeque<Left>;

    // Retrieve the right output queue
    fn right_output(&mut self) -> &mut VecDeque<Right>;

    // Work on an object, this method performs processing in a synchronous manner.
    fn work(&mut self, object: In) -> Result<(), Error>;

    // Flush the bifurcation, outputting available output and resetting internal state.
    fn flush(&mut self) -> Result<(), Error>;

    fn name(&self) -> &str {
        return "unknown";
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub struct MockBifurcation {
        shared_state: usize,
        left_out: VecDeque<usize>,
        right_out: VecDeque<usize>,
    }

    impl MockBifurcation {
        pub fn new() -> Self {
            MockBifurcation {
                shared_state: 0,
                left_out: VecDeque::new(),
                right_out: VecDeque::new(),
            }
        }
    }

    impl BifurcationRoutine<usize, usize, usize> for MockBifurcation {
        fn left_output(&mut self) -> &mut VecDeque<usize> {
            &mut self.left_out
        }

        fn right_output(&mut self) -> &mut VecDeque<usize> {
            &mut self.right_out
        }

        fn work(&mut self, object: usize) -> Result<(), Error> {
            self.left_out.push_back(object * 2 + self.shared_state);
            self.right_out.push_back(object * 3 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }

        fn flush(&mut self) -> Result<(), Error> {
            self.shared_state = 0;
            Ok(())
        }
    }
}
