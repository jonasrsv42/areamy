//! Threaded end-to-end: Flush then immediate close across sync and async threads.

use super::mock::{Hold, IoThread, drain_edge};
use crate::error::Error;
use crate::poll;
use crate::poll::future::line::FutureRoutine;
use crate::poll::future::queue::{Input, InputConsumer, OutputProducer};
use crate::sync::Receiver;
use crate::work::{Writer, make_line};
use crate::{
    Closeable, DefaultThread, Message, Pushable, ThreadBundle, ThreadStream, make_push, make_work,
};

/// Marks output that only exists because the future saw Flush.
const FLUSHED: usize = 999;

fn run_once() -> Result<(), Error> {
    let mut sync_node = make_line(Hold::new());
    let mut writer = Writer::new(&sync_node)?;

    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let routine = FutureRoutine::factory(
        |input: InputConsumer<usize>, output: OutputProducer<usize>| {
            Box::pin(async move {
                loop {
                    match input.recv().await? {
                        Input::Data(value) => output.push(value),
                        Input::Flush => {
                            output.push(FLUSHED);
                            return Ok(());
                        }
                    }
                }
            })
        },
    );
    let mut async_node = async_thread
        .line(routine)
        .input::<poll::Sync>()
        .output::<poll::Sync>();
    make_push(&mut sync_node, &async_node)?;

    let output = Receiver::new();
    make_push(&mut async_node, &output)?;
    async_thread.add(async_node);

    let mut sync_thread = ThreadStream::<'_, DefaultThread>::new();
    make_work(sync_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);

    std::thread::scope(|s| -> Result<(), Error> {
        let handle = bundle.start(s);

        writer.push(Message::Data(1))?;
        writer.push(Message::Data(2))?;
        writer.push(Message::Flush("f".into()))?;
        writer.close()?;

        assert_eq!(
            drain_edge(&output)?,
            vec![
                Message::Data(1),
                Message::Data(2),
                Message::Data(FLUSHED),
                Message::Flush("f".into()),
            ]
        );

        assert!(handle.join().errors().is_empty());
        Ok(())
    })
}

#[test]
fn sync_to_async_flush_then_immediate_close() -> Result<(), Error> {
    for _ in 0..10 {
        run_once()?;
    }
    Ok(())
}
