//! Every edge type drains its buffer before reporting Closed.

use super::mock::{Signal, is_closed};
use crate::Message;
use crate::connect::poll::edge::PollEdge;
use crate::connect::poll::input;
use crate::connect::waker::mock;
use crate::error::Error;
use crate::poll::future::queue::{Input, InputQueue};
use crate::sync::Receiver;
use std::pin::Pin;

fn assert_closed<T: std::fmt::Debug>(result: Result<T, Error>) {
    match result {
        Err(e) if is_closed(&e) => {}
        other => panic!("expected Closed, got {:?}", other),
    }
}

#[test]
fn sync_edge_read_front() -> Result<(), Error> {
    let rx = Receiver::<usize, Signal>::new();
    let tx = rx.sender();
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
    let tx = rx.sender();
    tx.push_back(Message::Flush("f".into()))?;
    tx.close()?;

    assert_eq!(rx.poll()?, Some(Message::Flush("f".into())));
    assert_closed(rx.poll());
    Ok(())
}

#[test]
fn sync_edge_read_all() -> Result<(), Error> {
    let rx = Receiver::<usize, Signal>::new();
    let tx = rx.sender();
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
    let rx = input::sync::Receiver::<usize, Signal>::new(std::task::Waker::noop().clone());
    let tx = rx.sender();
    tx.push_back(Message::Data(1))?;
    tx.push_back(Message::Flush("f".into()))?;
    tx.close()?;

    assert_closed(tx.push_back(Message::Data(2)));
    assert_eq!(rx.try_recv()?, Some(Message::Data(1)));
    assert_eq!(rx.try_recv()?, Some(Message::Flush("f".into())));
    assert_closed(rx.try_recv());
    Ok(())
}

#[test]
fn poll_local_edge() -> Result<(), Error> {
    let mut edge = PollEdge::<usize, Signal>::new(mock::noop_local_waker());
    edge.push(Message::Data(1))?;
    edge.push(Message::Flush("f".into()))?;
    edge.close()?;

    assert_closed(edge.push(Message::Data(2)));
    assert_eq!(edge.try_recv()?, Some(Message::Data(1)));
    assert_eq!(edge.try_recv()?, Some(Message::Flush("f".into())));
    assert_closed(edge.try_recv());
    Ok(())
}

/// The future input queue closes itself on Flush and reopens on reset.
#[test]
fn future_input_queue() -> Result<(), Error> {
    let queue = InputQueue::<usize>::new(mock::noop_local_waker());
    let waker = std::task::Waker::noop();
    let mut cx = core::task::Context::from_waker(waker);
    queue.producer.push(Input::Data(1));
    queue.producer.push(Input::Flush);

    let mut recv = queue.consumer.recv();
    assert!(matches!(
        Pin::new(&mut recv).poll(&mut cx),
        core::task::Poll::Ready(Ok(Input::Data(1)))
    ));
    let mut recv = queue.consumer.recv();
    assert!(matches!(
        Pin::new(&mut recv).poll(&mut cx),
        core::task::Poll::Ready(Ok(Input::Flush))
    ));
    let mut recv = queue.consumer.recv();
    match Pin::new(&mut recv).poll(&mut cx) {
        core::task::Poll::Ready(result) => assert_closed(result.map(|_| ())),
        core::task::Poll::Pending => panic!("expected Closed after Flush"),
    }
    queue.producer.reset()
}
