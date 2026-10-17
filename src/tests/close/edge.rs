//! Every edge type drains its buffer before reporting Closed.

use super::mock::Signal;
use crate::Message;
use crate::error::Error;
use crate::graph::{Closeable, Receivable, TryPush, TryPushable};
use crate::node::routine::Accept;
use crate::poll::edge;
use crate::poll::future::{self, Input};
use crate::poll::input;
use crate::poll::timer::mock::noop_timer;
use crate::sync::Receiver;
use crate::wake::mock;
use std::pin::Pin;

fn assert_closed<T: std::fmt::Debug>(result: Result<T, Error>) {
    match result {
        Err(e) if e.is_closed() => {}
        other => panic!("expected Closed, got {:?}", other),
    }
}

#[test]
fn sync_edge_read_front() -> Result<(), Error> {
    let rx = Receiver::<usize, Signal>::new();
    let mut tx = rx.sender();
    tx.push_back(Message::Data(1))?;
    tx.push_back(Message::Flush("f".into()))?;
    tx.close()?;

    assert_closed(tx.push_back(Message::Data(2)));
    assert_eq!(rx.read_front()?, Message::Data(1));
    assert_eq!(rx.read_front()?, Message::Flush("f".into()));
    assert_closed(rx.read_front());
    Ok(())
}

#[test]
fn sync_edge_poll() -> Result<(), Error> {
    let rx = Receiver::<usize, Signal>::new();
    let mut tx = rx.sender();
    tx.push_back(Message::Flush("f".into()))?;
    tx.close()?;

    assert_eq!(rx.poll()?, Some(Message::Flush("f".into())));
    assert_closed(rx.poll());
    Ok(())
}

#[test]
fn sync_edge_read_all() -> Result<(), Error> {
    let rx = Receiver::<usize, Signal>::new();
    let mut tx = rx.sender();
    tx.push_back(Message::Data(1))?;
    tx.push_back(Message::Flush("f".into()))?;
    tx.close()?;

    assert_eq!(
        rx.read_all()?,
        vec![Message::Data(1), Message::Flush("f".into())]
    );
    assert_closed(rx.read_all());
    Ok(())
}

/// No producer can ever wake a blocking read, so it reports Closed.
#[test]
fn sync_edge_read_without_producers_is_closed() {
    let rx = Receiver::<usize, Signal>::new();
    assert_closed(rx.read_front());
    assert_closed(rx.wait_front());
}

#[test]
fn sync_edge_close_by_drop() -> Result<(), Error> {
    let rx = Receiver::<usize, Signal>::new();
    let tx = rx.sender();
    tx.push_back(Message::Flush("f".into()))?;
    drop(tx);

    assert_eq!(rx.read_front()?, Message::Flush("f".into()));
    assert_closed(rx.read_front());
    Ok(())
}

#[test]
fn poll_input_edge() -> Result<(), Error> {
    let rx = input::sync::Receiver::<usize, Signal>::new(std::task::Waker::noop().clone(), None);
    let mut tx = rx.sender();
    tx.push_back(Message::Data(1))?;
    tx.push_back(Message::Flush("f".into()))?;
    tx.close()?;

    assert_closed(tx.push_back(Message::Data(2)));
    assert_eq!(rx.poll()?, Some(Message::Data(1)));
    assert_eq!(rx.poll()?, Some(Message::Flush("f".into())));
    assert_closed(rx.poll());
    Ok(())
}

#[test]
fn poll_local_edge() -> Result<(), Error> {
    let (mut tx, mut rx) = edge::channel::<usize, Signal>(mock::noop_local_waker(), None);
    assert_eq!(tx.try_push(Message::Data(1))?, TryPush::Pushed);
    assert_eq!(tx.try_push(Message::Flush("f".into()))?, TryPush::Pushed);
    tx.close()?;

    assert_closed(tx.try_push(Message::Data(2)).map(|_| ()));
    assert_eq!(rx.try_recv()?, Some(Message::Data(1)));
    assert_eq!(rx.try_recv()?, Some(Message::Flush("f".into())));
    assert_closed(rx.try_recv());
    Ok(())
}

/// The future input queue closes itself on Flush; renew opens the next segment's.
#[test]
fn future_input_queue() -> Result<(), Error> {
    let (mut sender, receiver) = future::input::channel(
        mock::noop_local_waker(),
        mock::noop_waker(),
        noop_timer(),
        None,
    );
    let waker = std::task::Waker::noop();
    let mut cx = core::task::Context::from_waker(waker);
    assert_eq!(sender.try_send(1)?, Accept::Accepted);
    sender.flush()?;

    let mut recv = receiver.recv();
    assert!(matches!(
        Pin::new(&mut recv).poll(&mut cx),
        core::task::Poll::Ready(Ok(Input::Data(1)))
    ));
    let mut recv = receiver.recv();
    assert!(matches!(
        Pin::new(&mut recv).poll(&mut cx),
        core::task::Poll::Ready(Ok(Input::Flush))
    ));
    let mut recv = receiver.recv();
    match Pin::new(&mut recv).poll(&mut cx) {
        core::task::Poll::Ready(result) => assert_closed(result.map(|_| ())),
        core::task::Poll::Pending => panic!("expected Closed after Flush"),
    }
    sender.renew().map(|_| ())
}
