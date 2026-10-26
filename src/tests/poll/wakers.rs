//! A routine's `work` waker, as its node's builder hands it over, is the very one it's polled with:
//! a future parked on it is woken where the runtime polls.

use crate::error::Error;
use crate::poll;
use crate::poll::{BiunionWakers, Context, LineWakers};
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::Join;
use crate::wake::Waker;
use crate::work::Writer;
use crate::{Accept, At, Closeable, Message, Pushable, Trackable, Writes, biunion};
use alloc::sync::Arc;
use core::sync::atomic::{AtomicU8, Ordering};
use core::time::Duration;
use std::thread;
use std::time::Instant;

use super::mock::IoThread;

const UNPOLLED: u8 = 0;
const SAME: u8 = 1;
const OTHER: u8 = 2;

/// Asks to be polled on every message, then records whether its `work` waker is the poll's.
struct Probe {
    work: Waker,
    seen: Arc<AtomicU8>,
}

impl Probe {
    fn accept(&mut self) -> Result<Accept<usize>, Error> {
        self.work.local.wake();
        Ok(Accept::Accepted)
    }
}

impl crate::TrySend<usize> for Probe {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        self.accept()
    }
}

impl crate::TrySend<usize, biunion::Left> for Probe {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        self.accept()
    }
}

impl crate::TrySend<usize, biunion::Right> for Probe {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        self.accept()
    }
}

impl crate::Next<usize> for Probe {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl crate::Flush for Probe {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::Poll for Probe {
    fn poll(&mut self, cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        let same =
            self.work.sync.will_wake(&cx.wake.sync) && self.work.local.will_wake(&cx.wake.local);
        self.seen
            .store(if same { SAME } else { OTHER }, Ordering::SeqCst);
        Ok(core::task::Poll::Pending)
    }
}

impl poll::LineRoutine<usize, usize> for Probe {}
impl poll::BiunionRoutine<usize, usize, usize> for Probe {}

type Input = Writer<'static, usize, Trackable<&'static str>>;

/// Run `thread`, push one message through `writer`, and return what the probe saw. Closes `idle`
/// too: the node ends with its last input.
fn probe(
    thread: poll::Thread<'static, IoThread>,
    mut writer: Input,
    idle: Option<Input>,
    seen: &AtomicU8,
) -> u8 {
    thread::scope(|s| {
        let handle = thread.start(s);
        writer.push(Message::Data(1)).unwrap();
        let deadline = Instant::now() + DEADLINE / 2;
        while seen.load(Ordering::SeqCst) == UNPOLLED && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        writer.close().unwrap();
        if let Some(mut idle) = idle {
            idle.close().unwrap();
        }
        assert!(matches!(handle.join(), Join::Ok));
    });
    seen.load(Ordering::SeqCst)
}

#[test]
fn a_line_routine_is_polled_with_its_work_waker() {
    let _deadline = deadline(DEADLINE);
    let seen = Arc::new(AtomicU8::new(UNPOLLED));
    let record = seen.clone();
    let mut thread = poll::Thread::<IoThread>::new();
    let node = thread
        .line(move |wakers: LineWakers| Probe {
            work: wakers.work.wake,
            seen: record,
        })
        .input();
    let writer = node.writer().unwrap();
    thread.add(node);
    assert_eq!(probe(thread, writer, None, &seen), SAME);
}

#[test]
fn a_biunion_routine_is_polled_with_its_work_waker() {
    let _deadline = deadline(DEADLINE);
    for left in [true, false] {
        let seen = Arc::new(AtomicU8::new(UNPOLLED));
        let record = seen.clone();
        let mut thread = poll::Thread::<IoThread>::new();
        let node = thread
            .biunion(move |wakers: BiunionWakers| Probe {
                work: wakers.work.wake,
                seen: record,
            })
            .input::<biunion::Left>()
            .input::<biunion::Right>();
        let left_writer = node.at_ref::<biunion::Left>().writer().unwrap();
        let right_writer = node.at_ref::<biunion::Right>().writer().unwrap();
        thread.add(node);
        let (writer, idle) = match left {
            true => (left_writer, right_writer),
            false => (right_writer, left_writer),
        };
        let saw = probe(thread, writer, Some(idle), &seen);
        assert_eq!(
            saw,
            SAME,
            "fed on the {} side",
            if left { "left" } else { "right" }
        );
    }
}
