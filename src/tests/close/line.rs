//! Line nodes: work, pull, bridged, and poll.

use super::mock::{
    Hold, IoThread, Msg, Script, drain, drain_edge, echo, hold, is_closed, slow_hold,
};
use crate::edge::sync::tests::join_within;
use crate::error::Error;
use crate::poll::{self, LineWakers};
use crate::pull;
use crate::sync::Receiver;
use crate::work;
use crate::{Closeable, Message, Pullable, Pushable, Pushes, Reads, Via, Writes};
use std::thread;

fn flushed(messages: Vec<Msg>) {
    assert_eq!(
        messages,
        vec![
            Message::Data(1),
            Message::Data(2),
            Message::Flush("f".into())
        ]
    );
}

#[test]
fn work_line_flush_then_close() -> Result<(), Error> {
    let line = work::Line::of(Hold::new());
    let mut writer = line.writer()?;
    let mut reader = line.reader()?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Data(2))?;
    writer.push(Message::Flush("f".into()))?;
    writer.close()?;

    flushed(drain(&mut reader)?);
    Ok(())
}

#[test]
fn work_line_stacked_flush_then_close() -> Result<(), Error> {
    let first = work::Line::of(Hold::new());
    let mut writer = first.writer()?;
    let second = first.via(work::Line::of(Hold::new()))?;
    let mut reader = second.reader()?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Data(2))?;
    writer.push(Message::Flush("f".into()))?;
    writer.close()?;

    flushed(drain(&mut reader)?);
    Ok(())
}

#[test]
fn work_line_marker_then_close() -> Result<(), Error> {
    let line = work::Line::of(Hold::echo());
    let mut writer = line.writer()?;
    let mut reader = line.reader()?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Marker("m".into()))?;
    writer.close()?;

    assert_eq!(
        drain(&mut reader)?,
        vec![Message::Data(1), Message::Marker("m".into())]
    );
    Ok(())
}

#[test]
fn work_line_data_after_flush_then_close() -> Result<(), Error> {
    let line = work::Line::of(Hold::new());
    let mut writer = line.writer()?;
    let mut reader = line.reader()?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Flush("f".into()))?;
    writer.push(Message::Data(2))?;
    writer.close()?;

    assert_eq!(
        drain(&mut reader)?,
        vec![Message::Data(1), Message::Flush("f".into())]
    );
    Ok(())
}

#[test]
fn work_line_close_without_flush_keeps_nothing() -> Result<(), Error> {
    let line = work::Line::of(Hold::new());
    let mut writer = line.writer()?;
    let mut reader = line.reader()?;

    writer.push(Message::Data(1))?;
    writer.close()?;

    assert_eq!(drain(&mut reader)?, vec![]);
    Ok(())
}

/// One writer closing a fan-in edge closes it for every writer.
#[test]
fn work_line_fan_in_close_by_one_writer() -> Result<(), Error> {
    let line = work::Line::of(Hold::new());
    let mut a = line.writer()?;
    let mut b = line.writer()?;
    let mut reader = line.reader()?;

    a.push(Message::Data(1))?;
    b.push(Message::Data(2))?;
    a.push(Message::Flush("f".into()))?;
    a.close()?;
    assert!(b.push(Message::Data(3)).is_err());

    flushed(drain(&mut reader)?);
    Ok(())
}

fn script() -> Script {
    Script::new(vec![
        Message::Data(1),
        Message::Data(2),
        Message::Flush("f".into()),
    ])
}

#[test]
fn pull_line_flush_then_close() -> Result<(), Error> {
    let mut reader = pull::Reader::new(script().then(Hold::new()));
    flushed(drain(&mut reader)?);
    Ok(())
}

#[test]
fn bridged_line_flush_then_close() -> Result<(), Error> {
    let line = work::Pulled::of(script()).via(work::Line::of(Hold::new()))?;
    let mut reader = line.reader()?;
    flushed(drain(&mut reader)?);
    Ok(())
}

/// A pulled source closing only drops that source; the edge stays open
/// for the other one until every producer is gone.
#[test]
fn bridged_line_fan_in_first_close_keeps_edge_open() -> Result<(), Error> {
    let line = work::Pulled::of(Script::new(vec![])).via(work::Line::of(Hold::new()))?;
    let line = work::Pulled::of(script()).via(line)?;
    let mut reader = line.reader()?;

    flushed(drain(&mut reader)?);
    Ok(())
}

/// Push everything, then run the poll thread inline until it drains and exits.
fn poll_line<R>(routine: fn(LineWakers) -> R, messages: Vec<Msg>) -> Result<Vec<Msg>, Error>
where
    R: poll::LineRoutine<usize, usize> + 'static,
{
    let mut async_thread = poll::Thread::<IoThread>::new();
    let mut node = async_thread.line(routine).input().output();
    let mut writer = node.writer()?;
    let output = Receiver::new();
    node.pushes(&output)?;
    async_thread.add(node);

    for message in messages {
        writer.push(message)?;
    }
    writer.close()?;
    async_thread.run()?;
    drain_edge(&output)
}

fn flush_script() -> Vec<Msg> {
    vec![
        Message::Data(1),
        Message::Data(2),
        Message::Flush("f".into()),
    ]
}

#[test]
fn poll_line_flush_then_close() -> Result<(), Error> {
    flushed(poll_line(hold, flush_script())?);
    Ok(())
}

/// Close is already queued while the flush still needs several polls.
#[test]
fn poll_line_slow_flush_then_close() -> Result<(), Error> {
    flushed(poll_line(slow_hold, flush_script())?);
    Ok(())
}

#[test]
fn poll_line_multi_segment_then_close() -> Result<(), Error> {
    let out = poll_line(
        slow_hold,
        vec![
            Message::Data(1),
            Message::Flush("a".into()),
            Message::Data(2),
            Message::Flush("b".into()),
        ],
    )?;
    assert_eq!(
        out,
        vec![
            Message::Data(1),
            Message::Flush("a".into()),
            Message::Data(2),
            Message::Flush("b".into()),
        ]
    );
    Ok(())
}

/// Data first: a `pushes` edge only forwards a signal that follows data.
#[test]
fn poll_line_marker_then_close() -> Result<(), Error> {
    let out = poll_line(echo, vec![Message::Data(1), Message::Marker("m".into())])?;
    assert_eq!(out, vec![Message::Data(1), Message::Marker("m".into())]);
    Ok(())
}

#[test]
fn poll_line_data_after_flush_then_close() -> Result<(), Error> {
    let out = poll_line(
        hold,
        vec![
            Message::Data(1),
            Message::Flush("f".into()),
            Message::Data(2),
        ],
    )?;
    assert_eq!(out, vec![Message::Data(1), Message::Flush("f".into())]);
    Ok(())
}

#[test]
fn poll_line_close_without_flush_keeps_nothing() -> Result<(), Error> {
    assert_eq!(poll_line(hold, vec![Message::Data(1)])?, vec![]);
    Ok(())
}

/// A child closing first closes its parent through their local edge, while the parent's own
/// input stays open: the parent's next push finds the edge closed, and its input closes.
#[test]
fn poll_line_chain_child_closes_first() -> Result<(), Error> {
    let mut async_thread = poll::Thread::<IoThread>::new();
    let parent = async_thread.line(hold).input();
    let mut writer = parent.writer()?;
    let mut child = parent.via(async_thread.line(hold))?.output();
    {
        // Dropped at once: the child's first push finds its output closed.
        let output = Receiver::new();
        child.pushes(&output)?;
    }
    async_thread.add(child);

    // Never closes the writer: it stops only once the parent closes its input. Each flush
    // drives the parent's output.
    let producer = thread::spawn(move || {
        loop {
            for message in [Message::Data(1), Message::Flush("f".into())] {
                if let Err(error) = writer.push(message) {
                    return error;
                }
            }
            thread::yield_now();
        }
    });
    // Not `start`: its scope joins on exit, so a broken cascade would hang the suite instead of
    // failing `join_within`.
    let run = thread::spawn(move || async_thread.run());
    assert!(is_closed(&join_within(producer)));
    join_within(run)?;
    Ok(())
}

/// Parent and child poll lines joined by a local async edge.
#[test]
fn poll_line_chain_flush_then_close() -> Result<(), Error> {
    let mut async_thread = poll::Thread::<IoThread>::new();
    let parent = async_thread.line(slow_hold).input();
    let mut writer = parent.writer()?;
    let mut child = parent.via(async_thread.line(slow_hold))?.output();
    let output = Receiver::new();
    child.pushes(&output)?;
    async_thread.add(child);

    for message in flush_script() {
        writer.push(message)?;
    }
    writer.close()?;
    async_thread.run()?;
    flushed(drain_edge(&output)?);
    Ok(())
}
