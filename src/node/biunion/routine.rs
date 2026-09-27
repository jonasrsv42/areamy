use crate::node::{biunion, routine};

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a biunion routine from (`{Left}`, `{Right}`) to `{Out}`",
    note = "needs the supertraits of `BiunionRoutine` and an explicit `impl BiunionRoutine<{Left}, {Right}, {Out}> for {Self}`"
)]
pub trait BiunionRoutine<Left, Right, Out>:
    Send
    + routine::Send<Left, biunion::Left>
    + routine::Send<Right, biunion::Right>
    + routine::Next<Out>
    + routine::Flush
where
    Out: Clone,
{
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::error::Error;
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

    impl crate::Send<usize, biunion::Left> for MockBiunion {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.output.push_back(message * 2 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }
    }

    impl crate::Send<usize, biunion::Right> for MockBiunion {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.output.push_back(message * 3 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }
    }

    impl crate::Next<usize> for MockBiunion {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.output.pop_front())
        }
    }

    impl crate::Flush for MockBiunion {
        fn flush(&mut self) -> Result<(), Error> {
            self.shared_state = 0;
            Ok(())
        }
    }

    impl BiunionRoutine<usize, usize, usize> for MockBiunion {}
}
