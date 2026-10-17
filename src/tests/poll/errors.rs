//! Errors a routine's poll returns: passed through, except `Closed`, which is a bug.

use crate::error::{Error, ErrorKind};
use crate::poll;
use crate::poll::{Context, LineWakers};
use crate::tests::support::{DEADLINE, deadline};
use crate::wake::LocalWaker;
use crate::work;
use crate::{Accept, DefaultThread, Message, Pushable, Pushes, Streams, ThreadBundle, Writes};
use crate::{closed, fatal};
use std::sync::mpsc;

use super::mock::{Double, IoThread};

/// Accepts everything and asks to be polled, then fails its poll with the error `fail` makes.
struct Failing {
    work: LocalWaker,
    fail: fn() -> Error,
}

impl crate::TrySend<usize> for Failing {
    fn try_send(&mut self, _: usize) -> Result<Accept<usize>, Error> {
        self.work.wake();
        Ok(Accept::Accepted)
    }
}

impl crate::Next<usize> for Failing {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(None)
    }
}

impl crate::Flush for Failing {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::Poll for Failing {
    fn poll(&mut self, _: &mut Context) -> Result<core::task::Poll<()>, Error> {
        Err((self.fail)())
    }
}

impl poll::LineRoutine<usize, usize> for Failing {}

/// The kinds of the errors the threads end with, after feeding a [Failing] one message.
fn ends_with(fail: fn() -> Error) -> Result<Vec<ErrorKind>, Error> {
    let mut writer_node = work::Line::of(Double::new());
    let mut writer = writer_node.writer()?;
    let mut async_thread = poll::Thread::<IoThread>::new();
    let node = async_thread
        .line(move |wakers: LineWakers| Failing {
            work: wakers.work.local,
            fail,
        })
        .input();
    writer_node.pushes(&node)?;
    async_thread.add(node);

    let (failed, on_failure) = mpsc::channel();
    let mut bundle = ThreadBundle::new();
    bundle
        .add(writer_node.stream::<DefaultThread>())
        .add(async_thread)
        .on_first_error(move |_| {
            let _ = failed.send(());
        });
    std::thread::scope(|s| {
        let handle = bundle.start(s);
        writer.push(Message::Data(1))?;
        // Close only once the routine failed: closing first would end the node unpolled.
        let _ = on_failure.recv_timeout(DEADLINE);
        drop(writer);
        let errors = handle.join().errors();
        Ok(errors.into_iter().map(|error| error.kind).collect())
    })
}

#[test]
fn a_routines_error_ends_its_thread_unchanged() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    let kinds = ends_with(|| fatal!("routine broke"))?;
    assert!(
        matches!(&kinds[..], [ErrorKind::Fatal(message)] if message == "routine broke"),
        "{kinds:?}"
    );
    Ok(())
}

#[test]
fn a_routine_returning_closed_is_a_fatal_error() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    let kinds = ends_with(|| closed!())?;
    assert!(matches!(&kinds[..], [ErrorKind::Fatal(_)]), "{kinds:?}");
    Ok(())
}
