//! The one place a push may block on a full edge.

/// Runs a blocking push `f`, logging "blocked" before and "unblocked" after (also when `f`
/// errors or panics). A "blocked" with no matching "unblocked" is a deadlock.
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

fn log(node: &str, edge: usize, state: &str) {
    #[cfg(not(feature = "silent"))]
    eprintln!("{node} edge {edge}: {state}");
    #[cfg(feature = "silent")]
    let _ = (node, edge, state);
}
