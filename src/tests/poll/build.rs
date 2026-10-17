//! Building: minting order, forgotten nodes, and optional stages.

use crate::error::Error;
use crate::poll;
use crate::signal::Trackable;
use crate::tests::support::{DEADLINE, deadline};
use crate::thread::Join;
use crate::work;
use crate::{Closeable, Message, Pushable, Tees, Via, Writes};

use super::mock::{IoThread, PollDouble};
use crate::poll::AsyncParent;

/// Nodes mint in any order: none holds the thread borrow. A poll node with `.output()` tees.
#[test]
fn mint_then_wire() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    let mut thread = poll::Thread::<IoThread>::new();
    let a = thread.line(PollDouble::new);
    let b = thread.line(PollDouble::new);
    let c = thread.line(PollDouble::new);

    let a = a.input();
    let mut writer = a.writer()?;
    let mut tail = a.via(b)?.via(c)?.output();
    let mut reader = tail.tee()?;
    thread.add(tail);

    std::thread::scope(|s| -> Result<(), Error> {
        let handle = thread.start(s);
        writer.push(Message::Data(1))?;
        assert_eq!(reader.read()?, Message::Data(8));
        writer.close()?;
        assert!(matches!(handle.join(), Join::Ok));
        Ok(())
    })
}

/// A minted node never wired nor added leaves its reserved slot empty: the thread fails at build.
#[test]
fn forgotten_deferred_node_fails_at_build() -> Result<(), Error> {
    let mut thread = poll::Thread::<IoThread>::new();
    let head = thread.line(PollDouble::new).input();
    let mut writer = head.writer::<usize>()?;
    let tail = head.via(thread.line(PollDouble::new))?;
    thread.add(tail);
    let _orphan = thread.line::<usize, usize, Trackable<&'static str>, _>(PollDouble::new);

    std::thread::scope(|s| {
        let handle = thread.start(s);
        let _ = writer.close();
        match handle.join() {
            Join::Error(e) => assert!(e.to_string().contains("is empty"), "{e}"),
            _ => panic!("expected a slot-empty build error"),
        }
    });
    Ok(())
}

type Upstream<'params> = Box<
    dyn AsyncParent<
            'params,
            OutType = usize,
            SignalType = Trackable<&'static str>,
            ThreadIdType = IoThread,
        > + 'params,
>;

/// An optional stage before `core`, chosen at runtime behind one box: x2 core, x2 tail, and x2
/// more with the stage.
fn optional_stage(stage: bool) -> Result<usize, Error> {
    let mut thread = poll::Thread::<IoThread>::new();
    let core = thread.line(PollDouble::new);
    let (mut writer, core): (work::Writer<usize>, Upstream) = if stage {
        let pre = thread.line(PollDouble::new).input();
        (pre.writer()?, Box::new(pre.via(core)?))
    } else {
        let core = core.input();
        (core.writer()?, Box::new(core))
    };
    let mut tail = core.via(thread.line(PollDouble::new))?.output();
    let mut reader = tail.tee()?;
    thread.add(tail);

    std::thread::scope(|s| -> Result<usize, Error> {
        let handle = thread.start(s);
        writer.push(Message::Data(1))?;
        let delivered = reader.read()?;
        writer.close()?;
        assert!(matches!(handle.join(), Join::Ok));
        match delivered {
            Message::Data(value) => Ok(value),
            other => panic!("expected data, got {other:?}"),
        }
    })
}

#[test]
fn optional_upstream_stage() -> Result<(), Error> {
    let _deadline = deadline(DEADLINE);
    assert_eq!(optional_stage(false)?, 4);
    assert_eq!(optional_stage(true)?, 8);
    Ok(())
}
