use areamy::work::Line;
use areamy::{Pullable, Pushes, Reads, Streams, Via, Writes};
use std::collections::VecDeque;

pub struct AddOne {
    output: VecDeque<usize>,
}

impl Default for AddOne {
    fn default() -> Self {
        Self::new()
    }
}

impl AddOne {
    pub fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

impl areamy::Send<usize> for AddOne {
    fn send(&mut self, message: usize) -> Result<(), areamy::error::Error> {
        self.output.push_back(message + 1);
        Ok(())
    }
}

impl areamy::Next<usize> for AddOne {
    fn next(&mut self) -> Result<Option<usize>, areamy::error::Error> {
        Ok(self.output.pop_front())
    }
}

impl areamy::Flush for AddOne {
    fn flush(&mut self) -> Result<(), areamy::error::Error> {
        Ok(())
    }
}

impl areamy::LineRoutine<usize, usize> for AddOne {}

#[test]
fn simple_sync() -> Result<(), areamy::error::Error> {
    let inputs = Line::of(AddOne::new());
    let middle = Line::of(AddOne::new());
    let outputs = Line::of(AddOne::new());

    let writer = inputs.writer()?;
    let outputs = inputs.via(middle)?.via(outputs)?;
    let reader = outputs.reader()?;

    let mut io = areamy::LineIo::new(writer, reader);

    io.push(areamy::Message::Data(1))?;
    io.push(areamy::Message::Data(2))?;

    assert_eq!(io.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(io.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}

areamy::thread_id!(HelperThread);

#[test]
fn sync_multithread() -> Result<(), areamy::error::Error> {
    // Example of multithreaded graph.

    let inputs = Line::of(AddOne::new());
    let middle = Line::of(AddOne::new());
    let outputs = Line::of(AddOne::new());

    let writer = inputs.writer()?;
    let mut middle = inputs.via(middle)?;

    // Middle, on the `HelperThread`, pushes into outputs on the main thread.
    middle.pushes(&outputs)?;

    // The helper thread works on the inputs → middle subgraph.
    let helper_thread = middle.stream::<HelperThread>();

    let reader = outputs.reader()?;
    let mut io = areamy::LineIo::new(writer, reader);

    std::thread::scope(|s| {
        // Start the helper thread (consumes thread, returns handle).
        let _handle = helper_thread.start(s);

        // Helper thread will run the computation in the first two nodes.
        io.push(areamy::Message::Data(1)).unwrap();
        io.push(areamy::Message::Data(2)).unwrap();

        // Main thread runs computation in the final output node.
        assert_eq!(io.read().unwrap(), areamy::Message::Data(4));
        assert_eq!(io.read().unwrap(), areamy::Message::Data(5));

        // Close the writer so the helper thread can exit before scope
        // waits for it. Without this, scope would deadlock waiting for
        // a helper thread blocked on `wait_front()`.
        io.close().unwrap();
    });

    Ok(())
}

#[test]
fn simple_nosync() -> Result<(), areamy::error::Error> {
    // Nosync is useful to avoid unnecessary mutexes.
    // each connection is lockfree, at the cost of not being `Sync`.

    let inputs = areamy::pull::WriterBuffer::new();
    let writer = inputs.writer()?;

    let outputs = inputs
        .then(AddOne::new())
        .then(AddOne::new())
        .then(AddOne::new());

    let reader = areamy::pull::Reader::new(outputs);

    let mut io = areamy::LineIo::new(writer, reader);

    io.push(areamy::Message::Data(1))?;
    io.push(areamy::Message::Data(2))?;

    assert_eq!(io.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(io.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}
