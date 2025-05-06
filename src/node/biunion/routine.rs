use crate::biunion;

pub trait BiunionRoutine<Left, Right, Out>:
    Send
    + crate::Send<Left, biunion::Left>
    + crate::Send<Right, biunion::Right>
    + crate::Next<Out>
    + crate::Flush
    + crate::node::Name
where
    Out: Clone,
{
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

    impl crate::Send<usize, biunion::Left> for MockBiunion {
        fn send(&mut self, message: usize) -> Result<(), crate::error::Error> {
            self.output.push_back(message * 2 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }
    }

    impl crate::Send<usize, biunion::Right> for MockBiunion {
        fn send(&mut self, message: usize) -> Result<(), crate::error::Error> {
            self.output.push_back(message * 3 + self.shared_state);

            self.shared_state += 1;

            Ok(())
        }
    }

    impl crate::Next<usize> for MockBiunion {
        fn next(&mut self) -> Result<Option<usize>, crate::error::Error> {
            Ok(self.output.pop_front())
        }
    }

    impl crate::Flush for MockBiunion {
        fn flush(&mut self) -> Result<(), crate::error::Error> {
            self.shared_state = 0;
            Ok(())
        }
    }

    impl crate::node::Name for MockBiunion {
        fn name<'a>(&'a self) -> &'a str {
            "MockBiunion"
        }
    }

    impl BiunionRoutine<usize, usize, usize> for MockBiunion {}
}
