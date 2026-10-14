/// A poll node's flush and close lifecycle.
pub(crate) enum NodeState<SignalType> {
    Running,
    Flushing(SignalType),
    FlushReady(SignalType),
    MarkerReady(SignalType),
    CloseReady,
    Closed,
}
