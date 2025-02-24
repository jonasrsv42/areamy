use crate::bifurcation;

pub trait BifurcationRoutine<In, Left, Right>:
    Send
    + crate::Send<In>
    + crate::Next<Left, bifurcation::Left>
    + crate::Next<Right, bifurcation::Right>
    + crate::Flush
    + crate::node::Name
where
    In: Clone,
    Left: Clone,
    Right: Clone,
{
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::collections::VecDeque;
    use crate::error::Error;

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

    impl crate::node::Name for MockBifurcation {
        fn name<'a>(&'a self) -> &'a str {
            "MockBifurcation"
        }
    }

    impl BifurcationRoutine<usize, usize, usize> for MockBifurcation {}
}
