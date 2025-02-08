use areamy;

use std::collections::VecDeque;

pub struct AddOne {
    output: VecDeque<usize>,
}

impl AddOne {
    pub fn new() -> Result<Self, areamy::error::Error> {
        Ok(Self {
            output: VecDeque::new(),
        })
    }
}

impl areamy::LineRoutine<usize, usize> for AddOne {
    fn output(&mut self) -> &mut VecDeque<usize> {
        &mut self.output
    }

    fn work(&mut self, message: usize) -> Result<(), areamy::error::Error> {
        Ok(self.output.push_back(message + 1))
    }

    fn flush(&mut self) -> Result<(), areamy::error::Error> {
        Ok(())
    }
}

#[test]
fn simple_sync() -> Result<(), areamy::error::Error> {
    let mut in_node = areamy::sync::make_line(AddOne::new())?;
    let mut middle_node = areamy::sync::make_line(AddOne::new())?;
    let mut out_node = areamy::sync::make_line(AddOne::new())?;

    let source = areamy::sync::Source::<usize>::of(in_node.clone())?;

    areamy::sync::Connect::<usize>::bidi(&mut in_node, &mut middle_node)?;
    areamy::sync::Connect::<usize>::bidi(&mut middle_node, &mut out_node)?;

    let sink = areamy::sync::Sink::new(out_node.workable(), out_node.output())?;

    let mut reader = areamy::LineReader::new(source, sink);

    reader.push(areamy::Message::Data(1))?;
    reader.push(areamy::Message::Data(2))?;

    assert_eq!(reader.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(reader.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}

#[derive(Debug, Clone)]
struct HelperThread {}

impl areamy::ThreadId for HelperThread {}

#[test]
fn sync_multithread() -> Result<(), areamy::error::Error> {
    // Example of multithreaded graph.

    let mut in_node = areamy::sync::make_line(AddOne::new())?;
    let mut middle_node = areamy::sync::make_line(AddOne::new())?;
    let mut out_node = areamy::sync::make_line(AddOne::new())?;

    let source = areamy::sync::Source::<usize>::of(in_node.clone())?;
    areamy::sync::Connect::<usize>::bidi(&mut in_node, &mut middle_node)?;

    let mut helper_thread = areamy::ThreadStream::<HelperThread>::new();

    // Now helper thread will work on the middle_node subgraph.
    areamy::make_work(&middle_node, helper_thread.as_mut())?;

    // Ensure that middle node, using the `HelperThread` pushes data into out node.
    areamy::sync::Connect::<usize>::push(&mut middle_node, &out_node)?;

    let sink = areamy::sync::Sink::new(out_node.workable(), out_node.output())?;
    let mut reader = areamy::LineReader::new(source, sink);

    // Start the helper thread.
    helper_thread.start()?;

    // Helper thread will run the computation in the first two nodes.
    reader.push(areamy::Message::Data(1))?;
    reader.push(areamy::Message::Data(2))?;

    // Main thread runs computation in the final output node.
    assert_eq!(reader.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(reader.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}

#[test]
fn simple_nosync() -> Result<(), areamy::error::Error> {
    // Nosync is useful to avoid unnecessary mutexes.
    // each connection is lockfree, at the cost of not being `Sync`.

    let in_node = areamy::nosync::root(AddOne::new())?;

    let source = areamy::sync::Source::<usize>::of(in_node.input.clone())?;

    let middle_node = areamy::nosync::Connect::<usize>::pull(in_node, AddOne::new())?;
    let out_node = areamy::nosync::Connect::<usize>::pull(middle_node, AddOne::new())?;

    let sink = areamy::nosync::Sink::new(out_node);

    let mut reader = areamy::LineReader::new(source, sink);

    reader.push(areamy::Message::Data(1))?;
    reader.push(areamy::Message::Data(2))?;

    assert_eq!(reader.read().unwrap(), areamy::Message::Data(4));
    assert_eq!(reader.read().unwrap(), areamy::Message::Data(5));

    Ok(())
}
