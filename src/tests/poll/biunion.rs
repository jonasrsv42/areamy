//! Integration test: biunion node with audio data (left) + config updates (right).
//!
//! Demonstrates the core use case for biunion: a node that processes audio
//! frames while receiving config updates on a separate input. Config changes
//! the processing behavior (multiplier) without interrupting the data flow.

use crate::biunion;
use crate::error::Error;
use crate::poll;
use crate::poll::future;
use crate::poll::future::{Input, input, output};
use crate::sync::Receiver;
use crate::{At, Closeable, Message, Pushable, Pushes, Via, Writes};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

crate::thread_id!(IoThread);

/// Config update: just a multiplier.
#[derive(Clone, Debug)]
struct Config {
    multiplier: usize,
}

/// Audio data flows through left, config updates arrive on right.
/// The routine applies the current config multiplier to audio frames.
#[test]
fn biunion_audio_with_config() {
    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    let routine = future::biunion::FutureRoutine::factory(
        |audio: input::Receiver<usize>,
         config: input::Receiver<Config>,
         output: output::Sender<usize>| {
            Box::pin(async move {
                let multiplier = Rc::new(Cell::new(1usize));

                let audio_mul = multiplier.clone();
                let audio_out = output.clone();
                let audio_task: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        while let Input::Data(frame) = audio.recv().await? {
                            audio_out.send(frame * audio_mul.get()).await?;
                        }
                        Ok(())
                    });

                let config_mul = multiplier;
                let config_task: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        while let Ok(Input::Data(cfg)) = config.recv().await {
                            config_mul.set(cfg.multiplier);
                        }
                        Ok(())
                    });

                crate::poll::try_join(audio_task, config_task).await?;
                Ok(())
            })
        },
    );

    let mut node = async_thread
        .biunion(routine)
        .input::<biunion::Left, poll::Sync>()
        .input::<biunion::Right, poll::Sync>()
        .output::<poll::Sync>();

    let mut audio_writer = node.at_ref::<biunion::Left>().writer::<usize>().unwrap();
    let mut config_writer = node.at_ref::<biunion::Right>().writer::<Config>().unwrap();

    let output_edge = Receiver::new();
    node.pushes(&output_edge).unwrap();

    async_thread.add(node);
    std::thread::scope(|s| {
        let handle = async_thread.start(s);

        // Send audio with default multiplier (1)
        audio_writer.push(Message::Data(10)).unwrap();
        assert_eq!(output_edge.read_front().unwrap(), Message::Data(10)); // 10 * 1

        // Update config: multiplier = 3
        config_writer
            .push(Message::Data(Config { multiplier: 3 }))
            .unwrap();

        // Send marker on config to synchronize — when it arrives on output,
        // we know the config update was processed.
        config_writer.push(Message::Marker("sync".into())).unwrap();
        assert!(matches!(
            output_edge.read_front().unwrap(),
            Message::Marker(_)
        ));

        // Send more audio with new multiplier
        audio_writer.push(Message::Data(10)).unwrap();
        assert_eq!(output_edge.read_front().unwrap(), Message::Data(30)); // 10 * 3

        // Close audio — should close the whole node (wakes right input too)
        audio_writer.close().unwrap();

        assert!(matches!(handle.join(), crate::thread::Join::Ok));
    });
}

/// Audio comes from an async parent (line node that doubles), config from sync.
/// parent (doubles) → biunion (applies config multiplier) → output
#[test]
fn biunion_with_async_parent() {
    let mut async_thread = poll::Thread::<'_, IoThread>::new();

    // Parent line node: doubles input
    let parent = async_thread
        .line(future::line::FutureRoutine::factory(
            |input: input::Receiver<usize>, output: output::Sender<usize>| {
                Box::pin(async move {
                    while let Input::Data(v) = input.recv().await? {
                        output.send(v * 2).await?;
                    }
                    Ok(())
                })
            },
        ))
        .input::<poll::Sync>();

    // Writer pushes into parent
    let mut audio_writer = parent.writer::<usize>().unwrap();

    // Biunion: left from async parent, right from sync config
    let biunion_routine = future::biunion::FutureRoutine::factory(
        |audio: input::Receiver<usize>,
         config: input::Receiver<Config>,
         output: output::Sender<usize>| {
            Box::pin(async move {
                let multiplier = Rc::new(Cell::new(1usize));

                let audio_mul = multiplier.clone();
                let audio_out = output.clone();
                let audio_task: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        while let Input::Data(frame) = audio.recv().await? {
                            audio_out.send(frame * audio_mul.get()).await?;
                        }
                        Ok(())
                    });

                let config_mul = multiplier;
                let config_task: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        while let Ok(Input::Data(cfg)) = config.recv().await {
                            config_mul.set(cfg.multiplier);
                        }
                        Ok(())
                    });

                crate::poll::try_join(audio_task, config_task).await?;
                Ok(())
            })
        },
    );

    let mut node = parent
        .via(async_thread.biunion(biunion_routine).at::<biunion::Left>())
        .unwrap()
        .input::<biunion::Right, poll::Sync>()
        .output::<poll::Sync>();

    let mut config_writer = node.at_ref::<biunion::Right>().writer::<Config>().unwrap();

    let output_edge = Receiver::new();
    node.pushes(&output_edge).unwrap();

    async_thread.add(node);
    std::thread::scope(|s| {
        let handle = async_thread.start(s);

        // Audio 5 → parent doubles → 10 → biunion (multiplier=1) → 10
        audio_writer.push(Message::Data(5)).unwrap();
        assert_eq!(output_edge.read_front().unwrap(), Message::Data(10));

        // Update config: multiplier = 3
        config_writer
            .push(Message::Data(Config { multiplier: 3 }))
            .unwrap();
        config_writer.push(Message::Marker("sync".into())).unwrap();
        assert!(matches!(
            output_edge.read_front().unwrap(),
            Message::Marker(_)
        ));

        // Audio 5 → parent doubles → 10 → biunion (multiplier=3) → 30
        audio_writer.push(Message::Data(5)).unwrap();
        assert_eq!(output_edge.read_front().unwrap(), Message::Data(30));

        audio_writer.close().unwrap();
        assert!(matches!(handle.join(), crate::thread::Join::Ok));
    });
}
