// Doc-test routines for `misuse.rs`, pulled in with `include!`; not a module.

struct Pass(std::collections::VecDeque<usize>);
impl areamy::Send<usize> for Pass {
    fn send(&mut self, message: usize) -> Result<(), areamy::error::Error> {
        self.0.push_back(message);
        Ok(())
    }
}
impl areamy::Next<usize> for Pass {
    fn next(&mut self) -> Result<Option<usize>, areamy::error::Error> {
        Ok(self.0.pop_front())
    }
}
impl areamy::Flush for Pass {
    fn flush(&mut self) -> Result<(), areamy::error::Error> {
        Ok(())
    }
}
impl areamy::LineRoutine<usize, usize> for Pass {}
fn pass() -> Pass {
    Pass(std::collections::VecDeque::new())
}

struct Text(std::collections::VecDeque<String>);
impl areamy::Send<String> for Text {
    fn send(&mut self, message: String) -> Result<(), areamy::error::Error> {
        self.0.push_back(message);
        Ok(())
    }
}
impl areamy::Next<String> for Text {
    fn next(&mut self) -> Result<Option<String>, areamy::error::Error> {
        Ok(self.0.pop_front())
    }
}
impl areamy::Flush for Text {
    fn flush(&mut self) -> Result<(), areamy::error::Error> {
        Ok(())
    }
}
impl areamy::LineRoutine<String, String> for Text {}
fn text() -> Text {
    Text(std::collections::VecDeque::new())
}

struct Split(std::collections::VecDeque<usize>);
impl areamy::Send<usize> for Split {
    fn send(&mut self, message: usize) -> Result<(), areamy::error::Error> {
        self.0.push_back(message);
        Ok(())
    }
}
impl areamy::Next<usize, areamy::bifurcation::Left> for Split {
    fn next(&mut self) -> Result<Option<usize>, areamy::error::Error> {
        Ok(self.0.front().copied())
    }
}
impl areamy::Next<usize, areamy::bifurcation::Right> for Split {
    fn next(&mut self) -> Result<Option<usize>, areamy::error::Error> {
        Ok(self.0.pop_front())
    }
}
impl areamy::Flush for Split {
    fn flush(&mut self) -> Result<(), areamy::error::Error> {
        Ok(())
    }
}
impl areamy::BifurcationRoutine<usize, usize, usize> for Split {}
fn split() -> Split {
    Split(std::collections::VecDeque::new())
}

struct Join(std::collections::VecDeque<usize>);
impl areamy::Send<usize, areamy::biunion::Left> for Join {
    fn send(&mut self, message: usize) -> Result<(), areamy::error::Error> {
        self.0.push_back(message);
        Ok(())
    }
}
impl areamy::Send<String, areamy::biunion::Right> for Join {
    fn send(&mut self, message: String) -> Result<(), areamy::error::Error> {
        self.0.push_back(message.len());
        Ok(())
    }
}
impl areamy::Next<usize> for Join {
    fn next(&mut self) -> Result<Option<usize>, areamy::error::Error> {
        Ok(self.0.pop_front())
    }
}
impl areamy::Flush for Join {
    fn flush(&mut self) -> Result<(), areamy::error::Error> {
        Ok(())
    }
}
impl areamy::BiunionRoutine<usize, String, usize> for Join {}
fn join() -> Join {
    Join(std::collections::VecDeque::new())
}

type BoxFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(), areamy::error::Error>>>,
>;
fn forward(
    input: areamy::poll::future::input::Receiver<usize>,
    output: areamy::poll::future::output::Sender<usize>,
) -> BoxFuture {
    Box::pin(async move {
        while let areamy::poll::future::Input::Data(value) = input.recv().await? {
            output.send(value).await?;
        }
        Ok(())
    })
}
