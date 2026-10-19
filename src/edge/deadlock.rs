//! The one place a push may block on a full edge, and where a poll Output logs parking on one.

/// Runs a blocking push `f`, logging "blocked" before and "unblocked" after (also when `f`
/// errors or panics). A "blocked" with no matching "unblocked" is a deadlock, or a producer
/// starved by others refilling the edge first.
pub(crate) fn deadlock<R>(node: &str, edge: usize, f: impl FnOnce() -> R) -> R {
    log(node, edge, "blocked");
    // Dropped after `f`, on every exit path.
    let _unblocked = Unblocked { node, edge };
    f()
}

struct Unblocked<'a> {
    node: &'a str,
    edge: usize,
}

impl Drop for Unblocked<'_> {
    fn drop(&mut self) {
        log(self.node, self.edge, "unblocked");
    }
}

/// A poll Output parked on a full edge: it doesn't block, but a "parked" with no matching
/// "resumed" is a stall.
pub(crate) fn parked(node: &str, edge: usize) {
    log(node, edge, "parked");
}

pub(crate) fn resumed(node: &str, edge: usize) {
    log(node, edge, "resumed");
}

/// Opt-in (`trace`): these fire on every full edge, which is normal operation.
fn log(node: &str, edge: usize, state: &str) {
    #[cfg(feature = "trace")]
    eprintln!("{node} edge {edge}: {state}");
    #[cfg(not(feature = "trace"))]
    let _ = (node, edge, state);
}
