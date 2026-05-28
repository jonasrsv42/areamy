//! Shared mock routines + thread markers for the lifetime test module.
//!
//! Each routine holds a non-`'static` borrow (`&'a usize`) — what makes
//! it interesting is that the borrow is held by a graph node and must
//! flow through the entire cascade (routine → node → thread → bundle).

use crate::biunion::poll::routine::BiunionRoutine as PollBiunionRoutine;
use crate::connect::waker::Waker;
use crate::error::Error;
use crate::marker::Connection;
use crate::poll::future::OutputQueue;
use crate::{
    BifurcationRoutine, BiunionRoutine, Closeable, LineRoutine, Message, Pushable, ThreadId,
    bifurcation, biunion,
};
use std::collections::VecDeque;

// ---- Thread markers -------------------------------------------------------

#[derive(Debug, Clone)]
pub(super) struct LifetimeThread;
impl ThreadId for LifetimeThread {}

#[derive(Debug, Clone)]
pub(super) struct LifetimePollThread;
impl ThreadId for LifetimePollThread {}

// ============================================================
// Borrowing line routine (sync + pull)
// ============================================================

/// Line routine that holds a borrowed config (&'a usize) and scales
/// each value by it. Used by line × Workable and line × Pullable.
pub(super) struct BorrowingLine<'a> {
    multiplier: &'a usize,
    out: VecDeque<usize>,
}

impl<'a> BorrowingLine<'a> {
    pub(super) fn new(multiplier: &'a usize) -> Self {
        Self {
            multiplier,
            out: VecDeque::new(),
        }
    }
}

impl<'a> crate::Send<usize> for BorrowingLine<'a> {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.out.push_back(message * *self.multiplier);
        Ok(())
    }
}

impl<'a> crate::Next<usize> for BorrowingLine<'a> {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.out.pop_front())
    }
}

impl<'a> crate::Flush for BorrowingLine<'a> {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> crate::node::Name for BorrowingLine<'a> {}

impl<'a> LineRoutine<usize, usize> for BorrowingLine<'a> {}

// ============================================================
// Borrowing line routine (poll variant — also implements Poll)
// ============================================================

/// Poll line routine. Uses [`OutputQueue`] so pushing data wakes the
/// output phase (required by the poll/line contract).
pub(super) struct PollBorrowingLine<'a> {
    pub(super) multiplier: &'a usize,
    pub(super) output: OutputQueue<usize>,
}

impl<'a> crate::Send<usize> for PollBorrowingLine<'a> {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.producer.push(message * *self.multiplier);
        Ok(())
    }
}

impl<'a> crate::Next<usize> for PollBorrowingLine<'a> {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl<'a> crate::Flush for PollBorrowingLine<'a> {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> crate::Poll for PollBorrowingLine<'a> {
    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        Ok(core::task::Poll::Pending)
    }
}

impl<'a> crate::node::Name for PollBorrowingLine<'a> {}

impl<'a> crate::poll::LineRoutine<usize, usize> for PollBorrowingLine<'a> {}

// ============================================================
// Borrowing biunion routine (sync)
// ============================================================

pub(super) struct BorrowingBiunion<'a> {
    pub(super) bias: &'a usize,
    pub(super) out: VecDeque<usize>,
}

impl<'a> crate::Send<usize, biunion::Left> for BorrowingBiunion<'a> {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.out.push_back(message + *self.bias);
        Ok(())
    }
}

impl<'a> crate::Send<usize, biunion::Right> for BorrowingBiunion<'a> {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.out.push_back(message * *self.bias);
        Ok(())
    }
}

impl<'a> crate::Next<usize> for BorrowingBiunion<'a> {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.out.pop_front())
    }
}

impl<'a> crate::Flush for BorrowingBiunion<'a> {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> crate::node::Name for BorrowingBiunion<'a> {}

impl<'a> BiunionRoutine<usize, usize, usize> for BorrowingBiunion<'a> {}

// ============================================================
// Borrowing biunion routine (poll)
// ============================================================

/// Poll biunion routine. Uses [`OutputQueue`] so pushing data wakes the
/// output phase.
pub(super) struct PollBorrowingBiunion<'a> {
    pub(super) bias: &'a usize,
    pub(super) output: OutputQueue<usize>,
}

impl<'a> crate::Send<usize, biunion::Left> for PollBorrowingBiunion<'a> {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        self.output.producer.push(m + *self.bias);
        Ok(())
    }
}

impl<'a> crate::Send<usize, biunion::Right> for PollBorrowingBiunion<'a> {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        self.output.producer.push(m * *self.bias);
        Ok(())
    }
}

impl<'a> crate::Next<usize> for PollBorrowingBiunion<'a> {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.consumer.pop())
    }
}

impl<'a> crate::Flush for PollBorrowingBiunion<'a> {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> crate::Poll for PollBorrowingBiunion<'a> {
    fn poll(&mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        Ok(core::task::Poll::Pending)
    }
}

impl<'a> crate::node::Name for PollBorrowingBiunion<'a> {}

impl<'a> PollBiunionRoutine<usize, usize, usize> for PollBorrowingBiunion<'a> {}

// ============================================================
// Borrowing bifurcation routine (sync)
// ============================================================

pub(super) struct BorrowingBifurcation<'a> {
    pub(super) threshold: &'a usize,
    pub(super) left: VecDeque<usize>,
    pub(super) right: VecDeque<usize>,
}

impl<'a> crate::Send<usize> for BorrowingBifurcation<'a> {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        if message > *self.threshold {
            self.right.push_back(message);
        } else {
            self.left.push_back(message);
        }
        Ok(())
    }
}

impl<'a> crate::Next<usize, bifurcation::Left> for BorrowingBifurcation<'a> {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.left.pop_front())
    }
}

impl<'a> crate::Next<usize, bifurcation::Right> for BorrowingBifurcation<'a> {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.right.pop_front())
    }
}

impl<'a> crate::Flush for BorrowingBifurcation<'a> {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> crate::node::Name for BorrowingBifurcation<'a> {}

impl<'a> BifurcationRoutine<usize, usize, usize> for BorrowingBifurcation<'a> {}

// ============================================================
// Borrowed closeable sink (for poll output Vec<Box<dyn ... + 'params>>)
// ============================================================

/// Closeable that holds a non-`'static` borrow. Used by the poll output
/// sink test to verify `Edge::Output<'params>` and its `Add` impl really
/// accept non-`'static` sinks.
pub(super) struct BorrowingSink<'a> {
    pub(super) _config: &'a usize,
    pub(super) forward:
        Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync>,
}

impl<'a> Connection for BorrowingSink<'a> {}

impl<'a> Pushable for BorrowingSink<'a> {
    type DataType = usize;
    type SignalType = &'static str;
    fn push(&mut self, msg: Message<usize, &'static str>) -> Result<(), Error> {
        self.forward.push(msg)
    }
}

impl<'a> Closeable for BorrowingSink<'a> {
    fn close(&mut self) -> Result<(), Error> {
        self.forward.close()
    }
}

/// Erase [`BorrowingSink<'a>`] to `Box<dyn Closeable + 'a>`. Hides the
/// object-lifetime default that would otherwise force the coercion to
/// `'static` at the call site.
pub(super) fn box_borrowing_sink<'a>(
    s: BorrowingSink<'a>,
) -> Box<dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync + 'a> {
    Box::new(s)
}
