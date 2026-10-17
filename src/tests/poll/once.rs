//! [once::FutureRoutine] through the real poll loop: one segment is delivered and its Flush
//! forwarded, the spent node closes cleanly, and input after the segment ends the thread with
//! [once::Exhausted].
//!
//! Reads poll with a deadline and never panic, so a failure tears down instead of hanging.

use crate::edge::sync::Receiver;
use crate::error::{Error, ErrorKind};
use crate::message::Message;
use crate::poll::future::line::once;
use crate::poll::future::{Input, input, output};
use crate::signal::Trackable;
use crate::work::Writer;
use crate::{Closeable, Pushable, Pushes, ThreadBundle, Writes, poll};
use core::future::Future;
use core::pin::Pin;
use core::time::Duration;
use std::thread;
use std::time::Instant;

crate::thread_id!(OnceThread);

type Signal = Trackable<&'static str>;
type Msg = Message<usize, Signal>;
type BoxFut<'params> = Pin<Box<dyn Future<Output = Result<(), Error>> + 'params>>;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Not `Clone`: the routine's whole point is that its closure can move it in.
struct Session {
    factor: usize,
}

/// A once node scaling by `session.factor` and by `*scale`, borrowed from the test's scope.
fn once_line<'params>(
    session: Session,
    scale: &'params usize,
) -> (
    poll::Thread<'params, OnceThread>,
    Writer<'params, usize, Signal>,
    Receiver<usize, Signal>,
) {
    let mut thread = poll::Thread::<OnceThread>::new();
    let mut node = thread
        .line(once::FutureRoutine::factory(
            move |input: input::Receiver<usize>,
                  output: output::Sender<usize>|
                  -> BoxFut<'params> {
                Box::pin(async move {
                    let session = session;
                    while let Input::Data(value) = input.recv().await? {
                        output.send(value * session.factor * *scale).await?;
                    }
                    Ok(())
                })
            },
        ))
        .input()
        .output();
    let writer = node.writer().unwrap();
    let output = Receiver::new();
    node.pushes(&output).unwrap();
    thread.add(node);
    (thread, writer, output)
}

/// The next message; `None` once the edge is closed and empty, or on timeout.
fn read(output: &Receiver<usize, Signal>) -> Option<Msg> {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        match output.poll() {
            Ok(Some(message)) => return Some(message),
            Ok(None) => thread::sleep(Duration::from_millis(1)),
            Err(_) => return None,
        }
    }
    None
}

fn read_n(output: &Receiver<usize, Signal>, count: usize) -> Vec<Msg> {
    (0..count).map_while(|_| read(output)).collect()
}

fn is_exhausted(errors: &[Error]) -> bool {
    matches!(errors, [e] if e.is::<once::Exhausted>())
}

/// Data then a Flush: the edges forward a signal only behind data ([FollowData]).
///
/// [FollowData]: crate::edge::policy::FollowData
fn segment(writer: &mut Writer<'_, usize, Signal>, values: &[usize]) {
    for value in values {
        writer.push(Message::Data(*value)).unwrap();
    }
    writer.push(Message::Flush(Trackable::new("end"))).unwrap();
}

#[test]
fn one_segment_is_delivered_then_close_is_clean() {
    let scale = 10;
    let (thread, mut writer, output) = once_line(Session { factor: 2 }, &scale);
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    thread::scope(|s| {
        let handle = bundle.start(s);
        segment(&mut writer, &[1, 2]);
        let delivered = read_n(&output, 3);
        writer.close().unwrap();
        let after_close = read(&output);
        let errors = handle.join().errors();

        assert_eq!(
            delivered,
            vec![
                Message::Data(20),
                Message::Data(40),
                Message::Flush(Trackable::new("end"))
            ]
        );
        assert_eq!(after_close, None);
        assert!(errors.is_empty(), "{errors:?}");
    });
}

#[test]
fn data_after_the_segment_ends_the_thread_as_exhausted() {
    let scale = 1;
    let (thread, mut writer, output) = once_line(Session { factor: 1 }, &scale);
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    thread::scope(|s| {
        let handle = bundle.start(s);
        segment(&mut writer, &[1]);
        let delivered = read_n(&output, 2);
        writer.push(Message::Data(2)).unwrap();
        // The edge drains before it closes, so the data reaches the routine first. Should the
        // routine take it, the close still ends the thread, cleanly: the assert below fails.
        writer.close().unwrap();
        let errors = handle.join().errors();

        assert_eq!(
            delivered,
            vec![Message::Data(1), Message::Flush(Trackable::new("end"))]
        );
        assert!(is_exhausted(&errors), "{errors:?}");
        // A typed error, not a Fatal message: callers match on it.
        assert!(!matches!(errors[0].kind, ErrorKind::Fatal(_)));
    });
}

#[test]
fn a_second_flush_ends_the_thread_as_exhausted() {
    let scale = 1;
    let (thread, mut writer, output) = once_line(Session { factor: 1 }, &scale);
    let mut bundle = ThreadBundle::new();
    bundle.add(thread);

    thread::scope(|s| {
        let handle = bundle.start(s);
        segment(&mut writer, &[1]);
        let delivered = read_n(&output, 2);
        writer
            .push(Message::Flush(Trackable::new("again")))
            .unwrap();
        writer.close().unwrap();
        let errors = handle.join().errors();

        assert_eq!(
            delivered,
            vec![Message::Data(1), Message::Flush(Trackable::new("end"))]
        );
        assert!(is_exhausted(&errors), "{errors:?}");
    });
}
