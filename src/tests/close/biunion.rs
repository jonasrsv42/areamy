//! Biunion nodes: the closing input's Flush survives, the other input does not.

use super::mock::{
    HoldBiunion, IoThread, Msg, RIGHT, Signal, drain, drain_edge, hold, hold_biunion,
    slow_hold_biunion,
};
use crate::error::Error;
use crate::poll::{self, BiunionWakers};
use crate::sync::Receiver;
use crate::work::{Reader, Writer, make_biunion};
use crate::{Closeable, Message, Pushable, biunion, make_push};

/// Whether the still-open right input's data got consumed before the
/// Flush depends on scheduling; only the closing side is contractual.
fn left_flushed(messages: Vec<Msg>) {
    let right_data = Message::Data(5 + RIGHT);
    let messages: Vec<Msg> = messages
        .into_iter()
        .filter(|message| *message != right_data)
        .collect();
    assert_eq!(messages, vec![Message::Data(1), Message::Flush("f".into())]);
}

fn right_flushed(messages: Vec<Msg>) {
    assert_eq!(
        messages,
        vec![Message::Data(5 + RIGHT), Message::Flush("f".into())]
    );
}

fn send<L, R>(close_left: bool, left: &mut L, right: &mut R) -> Result<(), Error>
where
    L: Pushable<DataType = usize, SignalType = Signal> + Closeable,
    R: Pushable<DataType = usize, SignalType = Signal> + Closeable,
{
    if close_left {
        left.push(Message::Data(1))?;
        right.push(Message::Data(5))?;
        left.push(Message::Flush("f".into()))?;
        left.close()
    } else {
        right.push(Message::Data(5))?;
        right.push(Message::Flush("f".into()))?;
        right.close()
    }
}

fn work_biunion(close_left: bool) -> Result<Vec<Msg>, Error> {
    let node = make_biunion(HoldBiunion::new());
    let mut left = Writer::new::<biunion::Left>(&node)?;
    let mut right = Writer::new::<biunion::Right>(&node)?;
    let mut reader = Reader::new(node)?;

    send(close_left, &mut left, &mut right)?;
    drain(&mut reader)
}

#[test]
fn work_biunion_close_left() -> Result<(), Error> {
    left_flushed(work_biunion(true)?);
    Ok(())
}

#[test]
fn work_biunion_close_right() -> Result<(), Error> {
    right_flushed(work_biunion(false)?);
    Ok(())
}

/// Push everything, then run the poll thread inline until it drains and exits.
fn poll_biunion<R>(routine: fn(BiunionWakers) -> R, close_left: bool) -> Result<Vec<Msg>, Error>
where
    R: crate::node::biunion::poll::routine::BiunionRoutine<usize, usize, usize> + 'static,
{
    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let mut node = async_thread
        .biunion(routine)
        .input::<biunion::Left, poll::Sync>()
        .input::<biunion::Right, poll::Sync>()
        .output::<poll::Sync>();
    let mut left = Writer::new::<biunion::Left>(&node)?;
    let mut right = Writer::new::<biunion::Right>(&node)?;
    let output = Receiver::new();
    make_push(&mut node, &output)?;
    async_thread.add(node);

    send(close_left, &mut left, &mut right)?;
    async_thread.run()?;
    drain_edge(&output)
}

#[test]
fn poll_biunion_close_left() -> Result<(), Error> {
    left_flushed(poll_biunion(hold_biunion, true)?);
    Ok(())
}

#[test]
fn poll_biunion_close_right() -> Result<(), Error> {
    right_flushed(poll_biunion(hold_biunion, false)?);
    Ok(())
}

/// Close is already queued while the flush still needs several polls.
#[test]
fn poll_biunion_slow_flush_close_left() -> Result<(), Error> {
    left_flushed(poll_biunion(slow_hold_biunion, true)?);
    Ok(())
}

#[test]
fn poll_biunion_slow_flush_close_right() -> Result<(), Error> {
    right_flushed(poll_biunion(slow_hold_biunion, false)?);
    Ok(())
}

/// Left input is an async parent line; closing it closes the biunion.
#[test]
fn poll_biunion_async_parent_flush_then_close() -> Result<(), Error> {
    let mut async_thread = poll::Thread::<'_, IoThread>::new();
    let parent = async_thread.line(hold).input::<poll::Sync>();
    let mut left = Writer::new(&parent)?;
    let mut node = async_thread
        .biunion(slow_hold_biunion)
        .parent::<biunion::Left>(parent)
        .input::<biunion::Right, poll::Sync>()
        .output::<poll::Sync>();
    let mut right = Writer::new::<biunion::Right>(&node)?;
    let output = Receiver::new();
    make_push(&mut node, &output)?;
    async_thread.add(node);

    send(true, &mut left, &mut right)?;
    async_thread.run()?;
    left_flushed(drain_edge(&output)?);
    Ok(())
}
