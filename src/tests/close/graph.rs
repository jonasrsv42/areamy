//! Threaded end-to-end: Flush then immediate close across sync and async threads.

use super::mock::{Hold, IoThread, drain_edge};
use crate::error::Error;
use crate::poll;
use crate::poll::future::line::FutureRoutine;
use crate::poll::future::{Input, input, output};
use crate::sync::Receiver;
use crate::work;
use crate::{Closeable, DefaultThread, Message, Pushable, Pushes, Streams, ThreadBundle, Writes};

/// Marks output that only exists because the future saw Flush.
const FLUSHED: usize = 999;

fn run_once() -> Result<(), Error> {
    let mut sync_node = work::Line::of(Hold::new());
    let mut writer = sync_node.writer()?;

    let mut async_thread = poll::Thread::<IoThread>::new();
    let routine = FutureRoutine::factory(
        |input: input::Receiver<usize>, output: output::Sender<usize>| {
            Box::pin(async move {
                loop {
                    match input.recv().await? {
                        Input::Data(value) => output.send(value).await?,
                        Input::Flush => {
                            output.send(FLUSHED).await?;
                            return Ok(());
                        }
                    }
                }
            })
        },
    );
    let mut async_node = async_thread.line(routine).input().output();
    sync_node.pushes(&async_node)?;

    let output = Receiver::new();
    async_node.pushes(&output)?;
    async_thread.add(async_node);

    let sync_thread = sync_node.stream::<DefaultThread>();

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
