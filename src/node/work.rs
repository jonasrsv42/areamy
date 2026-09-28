//! Helpers shared by the work nodes.

/// What a work node's search for its next message found.
pub(crate) enum Incoming<T> {
    /// A message to take.
    Message(T),
    /// The node's own input closed: nothing more will come.
    Closed,
}
