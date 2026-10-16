/// A poll node's flush and close lifecycle, shared by its Input, Work and Output phases.
pub(crate) enum NodeState<SignalType> {
    /// Input feeds the routine, Drive polls it, Output drains it. The only state Input reads in.
    Running,
    /// Input read a Flush and called `routine.flush()`; Drive polls until the routine is Ready.
    Flushing(SignalType),
    /// The routine finished its flush; Output drains what is left, then forwards the Flush.
    FlushReady(SignalType),
    /// Input read a Marker; Drive gives the routine one poll, then Output drains and forwards it.
    /// The routine never sees the Marker, so only a Flush can overshoot its bound.
    MarkerReady(SignalType),
    /// A Flush or Marker held in Output's fan-out, refused by an edge. Input and Drive wait; its
    /// delivery returns the node to Running and wakes the inputs and Work.
    Forwarding,
    /// An input closed, only ever from Running. Output delivers what the routine holds (an edge
    /// still refusing drops it), closes downstream, and the node closes.
    CloseReady,
    /// Closed, or failed: every phase returns `Closed`.
    Closed,
}
