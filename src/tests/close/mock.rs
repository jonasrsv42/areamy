use crate::error::{Error, ErrorKind};
use crate::marker::{Connection, Multiplicity};
use crate::node::Name;
use crate::poll::{BiunionWakers, LineWakers};
use crate::sync::Receiver;
use crate::{
    DefaultThread, Message, Pullable, ThreadId, Trackable, Workable, bifurcation, biunion, closed,
    reader,
};
use std::collections::VecDeque;

pub type Signal = Trackable<&'static str>;
pub type Msg = Message<usize, Signal>;

#[derive(Debug)]
pub struct IoThread;
impl ThreadId for IoThread {}

pub fn is_closed(error: &Error) -> bool {
    matches!(error.kind, ErrorKind::Closed)
}

/// Routine holding input until flush; output only exists because of Flush.
pub struct Hold {
    pending: VecDeque<usize>,
    ready: VecDeque<usize>,
    flushed: bool,
    echo: bool,
}

impl Hold {
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            ready: VecDeque::new(),
            flushed: false,
            echo: false,
        }
    }

    /// Emits input immediately instead of holding it.
    pub fn echo() -> Self {
        Self {
            echo: true,
            ..Self::new()
        }
    }

    fn release(&mut self) {
        self.ready.extend(self.pending.drain(..));
        self.flushed = true;
    }

    fn poll_flushed(&mut self) -> core::task::Poll<()> {
        if self.flushed {
            self.flushed = false;
            return core::task::Poll::Ready(());
        }
        core::task::Poll::Pending
    }
}

impl crate::Send<usize> for Hold {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        if self.echo {
            self.ready.push_back(message);
        } else {
            self.pending.push_back(message);
        }
        Ok(())
    }
}

impl crate::Next<usize> for Hold {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.ready.pop_front())
    }
}

impl crate::Flush for Hold {
    fn flush(&mut self) -> Result<(), Error> {
        self.release();
        Ok(())
    }
}

impl crate::Poll for Hold {
    fn poll(
        &mut self,
        _waker: &mut crate::connect::waker::Waker,
    ) -> Result<core::task::Poll<()>, Error> {
        Ok(self.poll_flushed())
    }
}

impl Name for Hold {}
impl crate::LineRoutine<usize, usize> for Hold {}
impl crate::poll::LineRoutine<usize, usize> for Hold {}

pub fn hold(_: LineWakers) -> Hold {
    Hold::new()
}

pub fn echo(_: LineWakers) -> Hold {
    Hold::echo()
}

/// Polls needed to finish a flush in [Slow].
pub const SLOW: usize = 3;

/// Poll routine whose flush stays Pending for [SLOW] polls, waking itself.
pub struct Slow<R> {
    inner: R,
    remaining: usize,
}

impl<R> Slow<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            remaining: 0,
        }
    }
}

impl<R, M> crate::Send<usize, M> for Slow<R>
where
    R: crate::Send<usize, M>,
    M: Multiplicity,
{
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.inner.send(message)
    }
}

impl<R: crate::Next<usize>> crate::Next<usize> for Slow<R> {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        self.inner.next()
    }
}

impl<R: crate::Flush> crate::Flush for Slow<R> {
    fn flush(&mut self) -> Result<(), Error> {
        self.remaining = SLOW;
        self.inner.flush()
    }
}

impl<R: crate::Poll> crate::Poll for Slow<R> {
    fn poll(
        &mut self,
        waker: &mut crate::connect::waker::Waker,
    ) -> Result<core::task::Poll<()>, Error> {
        if self.remaining > 0 {
            self.remaining -= 1;
            waker.local.wake();
            return Ok(core::task::Poll::Pending);
        }
        self.inner.poll(waker)
    }
}

impl<R> Name for Slow<R> {}
impl crate::poll::LineRoutine<usize, usize> for Slow<Hold> {}
impl crate::node::biunion::poll::routine::BiunionRoutine<usize, usize, usize>
    for Slow<HoldBiunion>
{
}

pub fn slow_hold(_: LineWakers) -> Slow<Hold> {
    Slow::new(Hold::new())
}

/// Right-side input is offset so the emitting side is visible.
pub const RIGHT: usize = 100;

pub struct HoldBiunion(Hold);

impl HoldBiunion {
    pub fn new() -> Self {
        Self(Hold::new())
    }
}

impl crate::Send<usize, biunion::Left> for HoldBiunion {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        crate::Send::send(&mut self.0, message)
    }
}

impl crate::Send<usize, biunion::Right> for HoldBiunion {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        crate::Send::send(&mut self.0, message + RIGHT)
    }
}

impl crate::Next<usize> for HoldBiunion {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        self.0.next()
    }
}

impl crate::Flush for HoldBiunion {
    fn flush(&mut self) -> Result<(), Error> {
        self.0.flush()
    }
}

impl crate::Poll for HoldBiunion {
    fn poll(
        &mut self,
        waker: &mut crate::connect::waker::Waker,
    ) -> Result<core::task::Poll<()>, Error> {
        self.0.poll(waker)
    }
}

impl Name for HoldBiunion {}
impl crate::BiunionRoutine<usize, usize, usize> for HoldBiunion {}
impl crate::node::biunion::poll::routine::BiunionRoutine<usize, usize, usize> for HoldBiunion {}

pub fn hold_biunion(_: BiunionWakers) -> HoldBiunion {
    HoldBiunion::new()
}

pub fn slow_hold_biunion(_: BiunionWakers) -> Slow<HoldBiunion> {
    Slow::new(HoldBiunion::new())
}

pub struct HoldBifurcation {
    pending: VecDeque<usize>,
    left: VecDeque<usize>,
    right: VecDeque<usize>,
}

impl HoldBifurcation {
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            left: VecDeque::new(),
            right: VecDeque::new(),
        }
    }
}

impl crate::Send<usize> for HoldBifurcation {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.pending.push_back(message);
        Ok(())
    }
}

impl crate::Next<usize, bifurcation::Left> for HoldBifurcation {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.left.pop_front())
    }
}

impl crate::Next<usize, bifurcation::Right> for HoldBifurcation {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.right.pop_front())
    }
}

impl crate::Flush for HoldBifurcation {
    fn flush(&mut self) -> Result<(), Error> {
        for value in self.pending.drain(..) {
            self.left.push_back(value);
            self.right.push_back(value + RIGHT);
        }
        Ok(())
    }
}

impl Name for HoldBifurcation {}
impl crate::BifurcationRoutine<usize, usize, usize> for HoldBifurcation {}

/// Scripted pull source: yields its messages, then Closed forever.
pub struct Script(VecDeque<Msg>);

impl Script {
    pub fn new(messages: Vec<Msg>) -> Self {
        Self(messages.into())
    }
}

impl Connection for Script {}

impl Pullable for Script {
    type ThreadId = DefaultThread;
    type DataType = usize;
    type SignalType = Signal;

    fn pull(&mut self) -> Result<Msg, Error> {
        self.0.pop_front().ok_or_else(|| closed!())
    }
}

/// Run a workable until it reports Closed.
pub fn drive_work(workable: &mut (impl Workable + ?Sized)) -> Result<(), Error> {
    loop {
        match workable.work() {
            Ok(()) => continue,
            Err(e) if is_closed(&e) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

fn collect(mut next: impl FnMut() -> Result<Msg, Error>) -> Result<Vec<Msg>, Error> {
    let mut out = Vec::new();
    loop {
        match next() {
            Ok(msg) => out.push(msg),
            Err(e) if is_closed(&e) => return Ok(out),
            Err(e) => return Err(e),
        }
    }
}

/// Read until Closed, collecting what came out.
pub fn drain(
    reader: &mut impl reader::Reader<DataType = usize, SignalType = Signal>,
) -> Result<Vec<Msg>, Error> {
    collect(|| reader.read())
}

/// Read a raw edge until Closed, collecting what came out.
pub fn drain_edge(receiver: &Receiver<usize, Signal>) -> Result<Vec<Msg>, Error> {
    collect(|| receiver.read_front())
}
