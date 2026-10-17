//! Biunion nodes: a side's close leaves the other running, the node ends with its last side, and
//! each side's flushed data survives, whichever closes first.

use super::mock::{
    HoldBiunion, IoThread, Msg, RIGHT, Signal, drain, drain_edge, hold, hold_biunion,
    slow_hold_biunion,
};
use crate::biunion::poll::routine::BiunionRoutine as PollBiunionRoutine;
use crate::error::Error;
use crate::poll::{self, BiunionWakers};
use crate::sync::Receiver;
use crate::work;
use crate::{At, Closeable, Message, Pushable, Pushes, Reads, Via, Writes, biunion};

/// Each side's data came out before its own Flush.
fn both_flushed(messages: Vec<Msg>) {
    for (data, name) in [(1, "l"), (5 + RIGHT, "r")] {
        let data = messages.iter().position(|m| *m == Message::Data(data));
        let flush = messages
            .iter()
            .position(|m| *m == Message::Flush(name.into()));
        assert!(
            matches!((data, flush), (Some(d), Some(f)) if d < f),
            "side {name} lost: {messages:?}"
        );
    }
}

fn right_flushed(messages: Vec<Msg>) {
    assert_eq!(
        messages,
        vec![Message::Data(5 + RIGHT), Message::Flush("f".into())]
    );
}

/// One side's segment, then its close.
fn segment(
    side: &mut (impl Pushable<DataType = usize, SignalType = Signal> + Closeable),
    value: usize,
    name: &'static str,
) -> Result<(), Error> {
    side.push(Message::Data(value))?;
    side.push(Message::Flush(name.into()))?;
    side.close()
}

/// Each side flushes and closes in turn: the first is closed before the other pushes anything.
fn send<L, R>(left_first: bool, left: &mut L, right: &mut R) -> Result<(), Error>
where
    L: Pushable<DataType = usize, SignalType = Signal> + Closeable,
    R: Pushable<DataType = usize, SignalType = Signal> + Closeable,
{
    if left_first {
        segment(left, 1, "l")?;
        segment(right, 5, "r")
    } else {
        segment(right, 5, "r")?;
        segment(left, 1, "l")
    }
}

fn work_biunion(left_first: bool) -> Result<Vec<Msg>, Error> {
    let node = work::Biunion::of(HoldBiunion::new());
    let mut left = node.at_ref::<biunion::Left>().writer()?;
    let mut right = node.at_ref::<biunion::Right>().writer()?;
    let mut reader = node.reader()?;

    send(left_first, &mut left, &mut right)?;
    drain(&mut reader)
}

#[test]
fn work_biunion_left_closes_first() -> Result<(), Error> {
    both_flushed(work_biunion(true)?);
    Ok(())
}

#[test]
fn work_biunion_right_closes_first() -> Result<(), Error> {
    both_flushed(work_biunion(false)?);
    Ok(())
}

/// Left never gets a writer. The node must still run on right, then
/// close when right does.
#[test]
fn work_biunion_unconnected_left() -> Result<(), Error> {
    let node = work::Biunion::of(HoldBiunion::new());
    let mut right = node.at_ref::<biunion::Right>().writer()?;
    let mut reader = node.reader()?;

    right.push(Message::Data(5))?;
    right.push(Message::Flush("f".into()))?;
    right.close()?;

    right_flushed(drain(&mut reader)?);
    Ok(())
}

/// Push everything, then run the poll thread inline until it drains and exits.
fn poll_biunion<R>(routine: fn(BiunionWakers) -> R, left_first: bool) -> Result<Vec<Msg>, Error>
where
    R: PollBiunionRoutine<usize, usize, usize> + 'static,
{
    let mut async_thread = poll::Thread::<IoThread>::new();
    let mut node = async_thread
        .biunion(routine)
        .input::<biunion::Left>()
        .input::<biunion::Right>()
        .output();
    let mut left = node.at_ref::<biunion::Left>().writer()?;
    let mut right = node.at_ref::<biunion::Right>().writer()?;
    let output = Receiver::new();
    node.pushes(&output)?;
    async_thread.add(node);

    send(left_first, &mut left, &mut right)?;
    async_thread.run()?;
    drain_edge(&output)
}

#[test]
fn poll_biunion_left_closes_first() -> Result<(), Error> {
    both_flushed(poll_biunion(hold_biunion, true)?);
    Ok(())
}

#[test]
fn poll_biunion_right_closes_first() -> Result<(), Error> {
    both_flushed(poll_biunion(hold_biunion, false)?);
    Ok(())
}

/// Closes are already queued while each flush still needs several polls.
#[test]
fn poll_biunion_slow_flush_left_closes_first() -> Result<(), Error> {
    both_flushed(poll_biunion(slow_hold_biunion, true)?);
    Ok(())
}

#[test]
fn poll_biunion_slow_flush_right_closes_first() -> Result<(), Error> {
    both_flushed(poll_biunion(slow_hold_biunion, false)?);
    Ok(())
}

/// Left input is an async parent line: its close ends the left side only.
#[test]
fn poll_biunion_async_parent_flush_then_close() -> Result<(), Error> {
    let mut async_thread = poll::Thread::<IoThread>::new();
    let parent = async_thread.line(hold).input();
    let mut left = parent.writer()?;
    let mut node = parent
        .via(
            async_thread
                .biunion(slow_hold_biunion)
                .at::<biunion::Left>(),
        )?
        .input::<biunion::Right>()
        .output();
    let mut right = node.at_ref::<biunion::Right>().writer()?;
    let output = Receiver::new();
    node.pushes(&output)?;
    async_thread.add(node);

    send(true, &mut left, &mut right)?;
    async_thread.run()?;
    both_flushed(drain_edge(&output)?);
    Ok(())
}
