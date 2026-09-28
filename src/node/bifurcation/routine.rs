use crate::node::{bifurcation, routine};

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a bifurcation routine from `{In}` to (`{Left}`, `{Right}`)",
    note = "needs the supertraits of `BifurcationRoutine` and an explicit `impl BifurcationRoutine<{In}, {Left}, {Right}> for {Self}`"
)]
pub trait BifurcationRoutine<In, Left, Right>:
    Send
    + routine::Send<In>
    + routine::Next<Left, bifurcation::Left>
    + routine::Next<Right, bifurcation::Right>
    + routine::Flush
where
    Left: Clone,
    Right: Clone,
{
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::error::Error;
    use std::collections::VecDeque;

    pub struct MockBifurcation {
        shared_state: usize,
        left_out: VecDeque<usize>,
        right_out: VecDeque<usize>,
    }

    impl Default for MockBifurcation {
        fn default() -> Self {
            Self::new()
        }
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

    impl crate::Send<usize> for MockBifurcation {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.left_out.push_back(message * 2 + self.shared_state);
            self.right_out.push_back(message * 3 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }
    }

    impl crate::Next<usize, bifurcation::Right> for MockBifurcation {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.right_out.pop_front())
        }
    }

    impl crate::Next<usize, bifurcation::Left> for MockBifurcation {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            Ok(self.left_out.pop_front())
        }
    }

    impl crate::Flush for MockBifurcation {
        fn flush(&mut self) -> Result<(), Error> {
            self.shared_state = 0;
            Ok(())
        }
    }

    impl BifurcationRoutine<usize, usize, usize> for MockBifurcation {}

    /// Sends each input to both sides unchanged; holds until `wait` have arrived or a flush,
    /// then stays open.
    pub struct HoldBifurcation {
        left: VecDeque<usize>,
        right: VecDeque<usize>,
        wait: usize,
        release: bool,
    }

    impl HoldBifurcation {
        pub fn new(wait: usize) -> Self {
            HoldBifurcation {
                left: VecDeque::new(),
                right: VecDeque::new(),
                wait,
                release: false,
            }
        }
    }

    impl crate::Send<usize> for HoldBifurcation {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            self.left.push_back(message);
            self.right.push_back(message);
            self.release |= self.left.len() >= self.wait;
            Ok(())
        }
    }

    impl crate::Next<usize, bifurcation::Left> for HoldBifurcation {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            if !self.release {
                return Ok(None);
            }
            Ok(self.left.pop_front())
        }
    }

    impl crate::Next<usize, bifurcation::Right> for HoldBifurcation {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            if !self.release {
                return Ok(None);
            }
            Ok(self.right.pop_front())
        }
    }

    impl crate::Flush for HoldBifurcation {
        fn flush(&mut self) -> Result<(), Error> {
            self.release = true;
            Ok(())
        }
    }

    impl BifurcationRoutine<usize, usize, usize> for HoldBifurcation {}
}
