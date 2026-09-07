//! Line nodes: work, pull, bridged, and poll.

use super::mock::{Hold, IoThread, Msg, Script, drain, drain_edge, echo, hold, slow_hold};
use crate::error::Error;
use crate::node::line::work::bridge::Bridge;
use crate::poll::{self, LineWakers};
use crate::pull;
use crate::sync::Receiver;
use crate::work::{Reader, Writer, from_pull, make_line};
use crate::{Closeable, Message, Pushable, make_bidi, make_push};

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
    let line = make_line(Hold::new());
    let mut writer = Writer::new(&line)?;
    let mut reader = Reader::new(line)?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Data(2))?;
    writer.push(Message::Flush("f".into()))?;
    writer.close()?;

    flushed(drain(&mut reader)?);
    Ok(())
}

#[test]
fn work_line_stacked_flush_then_close() -> Result<(), Error> {
    let first = make_line(Hold::new());
    let mut second = make_line(Hold::new());
    let mut writer = Writer::new(&first)?;
    make_bidi(first, &mut second)?;
    let mut reader = Reader::new(second)?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Data(2))?;
    writer.push(Message::Flush("f".into()))?;
    writer.close()?;

    flushed(drain(&mut reader)?);
    Ok(())
}

#[test]
fn work_line_marker_then_close() -> Result<(), Error> {
    let line = make_line(Hold::echo());
    let mut writer = Writer::new(&line)?;
    let mut reader = Reader::new(line)?;

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
    let line = make_line(Hold::new());
    let mut writer = Writer::new(&line)?;
    let mut reader = Reader::new(line)?;

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
    let line = make_line(Hold::new());
    let mut writer = Writer::new(&line)?;
    let mut reader = Reader::new(line)?;

    writer.push(Message::Data(1))?;
    writer.close()?;

    assert_eq!(drain(&mut reader)?, vec![]);
    Ok(())
}

/// One writer closing a fan-in edge closes it for every writer.
#[test]
fn work_line_fan_in_close_by_one_writer() -> Result<(), Error> {
    let line = make_line(Hold::new());
    let mut a = Writer::new(&line)?;
    let mut b = Writer::new(&line)?;
    let mut reader = Reader::new(line)?;

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
    let mut reader = pull::Reader::new(pull::make_pull(script(), Hold::new()));
    flushed(drain(&mut reader)?);
    Ok(())
}

#[test]
fn bridged_line_flush_then_close() -> Result<(), Error> {
    let mut reader = Reader::new(from_pull(script(), Hold::new()))?;
    flushed(drain(&mut reader)?);
    Ok(())
}

/// A bridge closing only drops that bridge; the Flush the other bridge
/// queued still goes out before the edge closes.
#[test]
fn bridged_line_two_bridges_keep_pending_flush() -> Result<(), Error> {
    let first = Script::new(vec![Message::Flush("f".into())]);
    let mut line = from_pull(first, Hold::new());
    let second = Bridge::new(Script::new(vec![]), line.input.sender());
    line.workers.push(Box::new(second));
    let mut reader = Reader::new(line)?;

    assert_eq!(drain(&mut reader)?, vec![Message::Flush("f".into())]);
    Ok(())
}

/// Push everything, then run the poll thread inline until it drains and exits.
fn poll_line<R>(routine: fn(LineWakers) -> R, messages: Vec<Msg>) -> Result<Vec<Msg>, Error>
where
    R: poll::LineRoutine<usize, usize> + 'static,
{
    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let mut node = async_thread
        .line(routine)
        .input::<poll::Sync>()
        .output::<poll::Sync>();
    let mut writer = Writer::new(&node)?;
    let output = Receiver::new();
    make_push(&mut node, &output)?;
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

/// Data first: the make_push edge only forwards a signal that follows data.
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

/// Parent and child poll lines joined by a local async edge.
#[test]
fn poll_line_chain_flush_then_close() -> Result<(), Error> {
    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let parent = async_thread.line(slow_hold).input::<poll::Sync>();
    let mut writer = Writer::new(&parent)?;
    let mut child = async_thread
        .line(slow_hold)
        .parent(parent)
        .output::<poll::Sync>();
    let output = Receiver::new();
    make_push(&mut child, &output)?;
    async_thread.add(child);

    for message in flush_script() {
        writer.push(message)?;
    }
    writer.close()?;
    async_thread.run()?;
    flushed(drain_edge(&output)?);
    Ok(())
}
