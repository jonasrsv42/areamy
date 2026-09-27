use areamy::Pullable;
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
    let in_node = areamy::work::Line::of(AddOne::new());
    let mut middle_node = areamy::work::Line::of(AddOne::new());
    let mut out_node = areamy::work::Line::of(AddOne::new());

    let writer = areamy::work::Writer::<usize>::of(&in_node)?;

    areamy::work::Bidi::<usize>::connect(in_node, &mut middle_node)?;
    areamy::work::Bidi::<usize>::connect(middle_node, &mut out_node)?;

    let reader = areamy::work::Reader::new(out_node)?;

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

    let in_node = areamy::work::Line::of(AddOne::new());
    let mut middle_node = areamy::work::Line::of(AddOne::new());
    let out_node = areamy::work::Line::of(AddOne::new());

    let writer = areamy::work::Writer::<usize>::of(&in_node)?;
    areamy::work::Bidi::<usize>::connect(in_node, &mut middle_node)?;

    // Ensure that middle node, using the `HelperThread` pushes data into out node.
    areamy::Push::<usize>::connect(&mut middle_node, &out_node)?;

    // Now helper thread will work on the middle_node subgraph.
    let helper_thread = areamy::ThreadStream::<HelperThread>::of(middle_node);

    let reader = areamy::work::Reader::new(out_node)?;
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

    let root = areamy::pull::WriterBuffer::new();
    let writer = areamy::work::Writer::<usize>::of(&root)?;

    let out_node = root
        .then(AddOne::new())
        .then(AddOne::new())
        .then(AddOne::new());

    let reader = areamy::pull::Reader::new(out_node);

    let mut io = areamy::LineIo::new(writer, reader);

    io.push(areamy::Message::Data(1))?;
    io.push(areamy::Message::Data(2))?;

    assert_eq!(io.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(io.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}
