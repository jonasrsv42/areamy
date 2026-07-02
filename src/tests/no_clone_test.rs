//! Non-cloneable data through a pullable pipeline.
//!
//! `Box<T>` is not [`Clone`] — these tests prove a pullable graph segment
//! can carry it. A chain of incrementers takes `Box<1>` and yields
//! `Box<n>` at the sink, with no cloning along the way.

use crate::Origin;
use crate::connect::graph::{Pullable, Pushable};
use crate::error::Error;
use crate::node::line::LineRoutine;
use crate::pull::{Reader, WriterBuffer, make_pull};
use crate::work::Writer;
use crate::{Message, Next, Send};
use std::collections::VecDeque;

struct BoxIncrementer {
    output: VecDeque<Box<usize>>,
}

impl BoxIncrementer {
    fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

impl Send<Box<usize>> for BoxIncrementer {
    fn send(&mut self, message: Box<usize>) -> Result<(), Error> {
        self.output.push_back(Box::new(*message + 1));
        Ok(())
    }
}

impl Next<Box<usize>> for BoxIncrementer {
    fn next(&mut self) -> Result<Option<Box<usize>>, Error> {
        Ok(self.output.pop_front())
    }
}

impl crate::Flush for BoxIncrementer {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::node::Name for BoxIncrementer {
    fn name(&self) -> &str {
        "BoxIncrementer"
    }
}

impl LineRoutine<Box<usize>, Box<usize>> for BoxIncrementer {}

/// Custom non-`Clone` signal type — used by [`test_non_cloneable_pull_signal`]
/// to prove the pipeline doesn't require `Clone` for signals either.
#[derive(Debug, PartialEq, Eq)]
struct BoxedOrigin(Box<usize>);

impl Origin for BoxedOrigin {}

impl From<Box<usize>> for BoxedOrigin {
    fn from(boxed: Box<usize>) -> Self {
        BoxedOrigin(boxed)
    }
}

#[test]
fn test_non_cloneable_pull_pipeline() {
    let buffer = WriterBuffer::new();
    let mut writer = Writer::new(&buffer).unwrap();

    // Box<1> → +1 → +1 → +1 → Box<4>
    let line1 = make_pull(buffer, BoxIncrementer::new());
    let line2 = make_pull(line1, BoxIncrementer::new());
    let line3 = make_pull(line2, BoxIncrementer::new());
    let mut reader = Reader::new(line3);

    writer.push(Message::Data(Box::new(1))).unwrap();

    let result = Pullable::pull(&mut reader).unwrap();
    assert!(matches!(result.data().map(|b| *b), Some(4)));
}

#[test]
fn test_non_cloneable_pull_signal() {
    let buffer: WriterBuffer<Box<usize>, BoxedOrigin, _> = WriterBuffer::new();
    let mut writer = Writer::of(&buffer).unwrap();

    let line1 = make_pull(buffer, BoxIncrementer::new());
    let line2 = make_pull(line1, BoxIncrementer::new());
    let mut reader = Reader::new(line2);

    writer.push(Message::Data(Box::new(42))).unwrap();

    let result = Pullable::pull(&mut reader).unwrap();
    assert!(matches!(result.data().map(|b| *b), Some(44)));
}
