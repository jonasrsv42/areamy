//! Test support shared across the crate.

use core::time::Duration;
use std::io::{self, Write};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;

/// Generous for any test here: the slowest finish well under a second.
pub(crate) const DEADLINE: Duration = Duration::from_secs(10);

/// Ends the test process if the test holding it runs past its limit, so a lost wake or a close
/// that never comes fails loudly instead of hanging the suite. Drop it to disarm; a test that
/// returns or panics drops it.
#[must_use = "the deadline is disarmed when dropped: hold it for the whole test"]
pub(crate) struct Deadline {
    /// Dropped with the guard: the watchdog's `recv` then disconnects and it exits quietly.
    _disarm: mpsc::Sender<()>,
}

/// Arm a watchdog for the calling test. It can't stop a hung test, only end the process, so on
/// expiry it names the test (libtest names each test's thread after it) and aborts.
///
/// Every test that blocks or waits on another thread takes one: `let _deadline =
/// deadline(DEADLINE);`. Under a mutant that jams every edge, the first such test to start
/// (libtest starts them in name order) aborting is what turns a mutation-sweep timeout into a
/// catch.
pub(crate) fn deadline(limit: Duration) -> Deadline {
    let (disarm, armed) = mpsc::channel::<()>();
    let test = thread::current()
        .name()
        .unwrap_or("an unnamed test")
        .to_owned();
    thread::spawn(move || {
        if let Err(RecvTimeoutError::Timeout) = armed.recv_timeout(limit) {
            // Straight to stderr: `eprintln!` here would go to libtest's capture for this test,
            // which the abort throws away.
            let message = format!("\n{test} ran past its {limit:?} deadline: aborting\n\n");
            let _ = io::stderr().write_all(message.as_bytes());
            std::process::abort();
        }
    });
    Deadline { _disarm: disarm }
}

/// A test that finishes in time disarms its watchdog by dropping the guard.
#[test]
fn a_test_within_its_deadline_is_left_alone() {
    let _deadline = deadline(Duration::from_millis(50));
    thread::sleep(Duration::from_millis(1));
}

/// Expiry ends the process, so it can't be asserted in-process: run this alone with
/// `--ignored` to see the named abort.
#[test]
#[ignore = "aborts the test process by design"]
fn a_test_past_its_deadline_aborts_the_process() {
    let _deadline = deadline(Duration::from_millis(50));
    thread::sleep(Duration::from_secs(5));
}
