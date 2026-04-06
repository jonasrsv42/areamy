//! Integration test: biunion node with audio data (left) + config updates (right).
//!
//! Demonstrates the core use case for biunion: a node that processes audio
//! frames while receiving config updates on a separate input. Config changes
//! the processing behavior (multiplier) without interrupting the data flow.

use areamy::biunion;
use areamy::error::Error;
use areamy::poll;
use areamy::poll::future::biunion::FutureRoutine;
use areamy::poll::future::queue::{Input, InputConsumer, OutputProducer};
use areamy::source::push::Source;
use areamy::{Closeable, Message, Pushable, SyncEdge, ThreadId, make_push};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug)]
struct IoThread;
impl ThreadId for IoThread {}

/// Config update: just a multiplier.
#[derive(Clone, Debug)]
struct Config {
    multiplier: usize,
}

/// Audio data flows through left, config updates arrive on right.
/// The routine applies the current config multiplier to audio frames.
#[test]
fn biunion_audio_with_config() {
    let mut async_thread = poll::Thread::<IoThread>::new();

    let routine = FutureRoutine::factory(
        |audio: InputConsumer<usize>,
         config: InputConsumer<Config>,
         output: OutputProducer<usize>| {
            Box::pin(async move {
                let multiplier = Rc::new(Cell::new(1usize));

                let audio_mul = multiplier.clone();
                let audio_out = output.clone();
                let audio_task: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        loop {
                            match audio.recv().await? {
                                Input::Data(frame) => {
                                    audio_out.push(frame * audio_mul.get());
                                }
                                Input::Flush => break,
                            }
                        }
                        Ok(())
                    });

                let config_mul = multiplier;
                let config_task: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        loop {
                            match config.recv().await {
                                Ok(Input::Data(cfg)) => {
                                    config_mul.set(cfg.multiplier);
                                }
                                _ => break,
                            }
                        }
                        Ok(())
                    });

                areamy::poll::Join::join([audio_task, config_task]).await
            })
        },
    );

    let mut node = async_thread.biunion(routine).left().right().output();

    let mut audio_source = Source::<usize>::of::<_, biunion::Left>(&node).unwrap();
    let mut config_source = Source::<Config>::of::<_, biunion::Right>(&node).unwrap();

    let output_edge = Arc::new(SyncEdge::new());
    make_push(&mut node, &output_edge).unwrap();

    async_thread.add(node);
    let handle = async_thread.start();

    // Send audio with default multiplier (1)
    audio_source.push(Message::Data(10)).unwrap();
    assert_eq!(output_edge.read_front().unwrap(), Message::Data(10)); // 10 * 1

    // Update config: multiplier = 3
    config_source
        .push(Message::Data(Config { multiplier: 3 }))
        .unwrap();

    // Send marker on config to synchronize — when it arrives on output,
    // we know the config update was processed.
    config_source.push(Message::Marker("sync".into())).unwrap();
    assert!(matches!(
        output_edge.read_front().unwrap(),
        Message::Marker(_)
    ));

    // Send more audio with new multiplier
    audio_source.push(Message::Data(10)).unwrap();
    assert_eq!(output_edge.read_front().unwrap(), Message::Data(30)); // 10 * 3

    // Close audio — should close the whole node (wakes right input too)
    audio_source.close().unwrap();

    let result = handle.join().unwrap();
    assert!(result.is_none());
}
