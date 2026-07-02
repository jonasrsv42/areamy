use std::collections::VecDeque;

pub struct AddOne {
    output: VecDeque<usize>,
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
        Ok(self.output.push_back(message + 1))
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

impl areamy::node::Name for AddOne {}
impl areamy::LineRoutine<usize, usize> for AddOne {}

#[test]
fn simple_sync() -> Result<(), areamy::error::Error> {
    let in_node = areamy::work::make_line(AddOne::new());
    let mut middle_node = areamy::work::make_line(AddOne::new());
    let mut out_node = areamy::work::make_line(AddOne::new());

    let writer = areamy::work::Writer::<usize>::of(&in_node)?;

    areamy::work::Connect::<usize>::bidi(in_node, &mut middle_node)?;
    areamy::work::Connect::<usize>::bidi(middle_node, &mut out_node)?;

    let reader = areamy::work::Reader::new(out_node)?;

    let mut io = areamy::LineIo::new(writer, reader);

    io.push(areamy::Message::Data(1))?;
    io.push(areamy::Message::Data(2))?;

    assert_eq!(io.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(io.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}

#[derive(Debug, Clone)]
struct HelperThread {}

impl areamy::ThreadId for HelperThread {}

#[test]
fn sync_multithread() -> Result<(), areamy::error::Error> {
    // Example of multithreaded graph.

    let in_node = areamy::work::make_line(AddOne::new());
    let mut middle_node = areamy::work::make_line(AddOne::new());
    let out_node = areamy::work::make_line(AddOne::new());

    let writer = areamy::work::Writer::<usize>::of(&in_node)?;
    areamy::work::Connect::<usize>::bidi(in_node, &mut middle_node)?;

    let mut helper_thread = areamy::ThreadStream::<'_, HelperThread>::new();

    // Ensure that middle node, using the `HelperThread` pushes data into out node.
    areamy::work::Connect::<usize>::push(&mut middle_node, &out_node)?;

    // Now helper thread will work on the middle_node subgraph.
    areamy::make_work(middle_node, &mut helper_thread)?;

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

    let in_node = areamy::pull::Connect::<usize>::pull(root, AddOne::new());
    let middle_node = areamy::pull::Connect::<usize>::pull(in_node, AddOne::new());
    let out_node = areamy::pull::Connect::<usize>::pull(middle_node, AddOne::new());

    let reader = areamy::pull::Reader::new(out_node);

    let mut io = areamy::LineIo::new(writer, reader);

    io.push(areamy::Message::Data(1))?;
    io.push(areamy::Message::Data(2))?;

    assert_eq!(io.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(io.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}
