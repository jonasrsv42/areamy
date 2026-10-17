//! Merges: a node fed by several sources ends with its last, and every source's flushed data
//! arrives, whatever order they close in.

use super::mock::{Hold, IoThread, Msg, Signal, drain, drain_edge, hold, run_poll};
use crate::edge::sync::tests::join_within;
use crate::error::Error;
use crate::message::Message;
use crate::poll;
use crate::sync::Receiver;
use crate::work;
use crate::{Closeable, Pushable, Pushes, Reads, Via, Writes};
use std::thread;

/// A source's segment: its values, then a Flush named for it.
struct Source {
    values: &'static [usize],
    name: &'static str,
}

/// Short: done, flushed and closed while [B] is still mid-segment.
const A: Source = Source {
    values: &[1],
    name: "a",
};

/// Long: still travelling when [A] closes.
const B: Source = Source {
    values: &[2, 3, 4, 5, 6],
    name: "b",
};

/// Push the source's segment, then close: a source done.
fn send(
    writer: &mut (impl Pushable<DataType = usize, SignalType = Signal> + Closeable),
    source: &Source,
) -> Result<(), Error> {
    for &value in source.values {
        writer.push(Message::Data(value))?;
    }
    writer.push(Message::Flush(source.name.into()))?;
    writer.close()
}

/// Every source's values came out, each before its own Flush.
fn delivered_both(out: &[Msg]) {
    for source in [&A, &B] {
        let flush = out
            .iter()
            .position(|m| *m == Message::Flush(source.name.into()));
        for &value in source.values {
            let data = out.iter().position(|m| *m == Message::Data(value));
            assert!(
                matches!((data, flush), (Some(d), Some(f)) if d < f),
                "segment {} lost {value}: {out:?}",
                source.name
            );
        }
    }
}

/// Both sources flush and close before the merge runs: [A] is done long before [B]'s Flush has
/// travelled, and B's segment must still arrive.
#[test]
fn a_work_merge_delivers_every_flushed_source() -> Result<(), Error> {
    let a = work::Line::of(Hold::new());
    let b = work::Line::of(Hold::new());
    let mut into_a = a.writer()?;
    let mut into_b = b.writer()?;
    let merge = a.via(work::Line::of(Hold::new()))?;
    let merge = b.via(merge)?;
    let reader = merge.reader()?;

    send(&mut into_a, &A)?;
    send(&mut into_b, &B)?;

    delivered_both(&drain(reader)?);
    Ok(())
}

/// Two writers into one node: the first to close leaves the other writing.
#[test]
fn a_work_merge_outlives_its_first_closed_writer() -> Result<(), Error> {
    let node = work::Line::of(Hold::new());
    let mut a = node.writer()?;
    let mut b = node.writer()?;
    let reader = node.reader()?;

    send(&mut a, &A)?;
    send(&mut b, &B)?;

    delivered_both(&drain(reader)?);
    Ok(())
}

/// Both sources push and close from their own threads while the merge runs: whatever the
/// interleaving, both flushed segments arrive.
#[test]
fn a_work_merge_delivers_racing_sources() -> Result<(), Error> {
    for _ in 0..200 {
        let node = work::Line::of(Hold::new());
        let mut a = node.writer()?;
        let mut b = node.writer()?;
        let reader = node.reader()?;
        let a = thread::spawn(move || send(&mut a, &A));
        let b = thread::spawn(move || send(&mut b, &B));
        let out = drain(reader)?;
        join_within(a)?;
        join_within(b)?;
        delivered_both(&out);
    }
    Ok(())
}

/// The poll twin: two parents into one poll line, both flushed and closed before it runs.
#[test]
fn a_poll_merge_delivers_every_flushed_source() -> Result<(), Error> {
    let mut thread = poll::Thread::<IoThread>::new();
    let a = thread.line(hold).input();
    let b = thread.line(hold).input();
    let mut into_a = a.writer()?;
    let mut into_b = b.writer()?;
    let merge = a.via(thread.line(hold))?;
    let mut merge = b.via(merge)?.output();
    let output = Receiver::new();
    merge.pushes(&output)?;
    thread.add(merge);

    send(&mut into_a, &A)?;
    send(&mut into_b, &B)?;
    run_poll(thread)?;

    delivered_both(&drain_edge(output)?);
    Ok(())
}
