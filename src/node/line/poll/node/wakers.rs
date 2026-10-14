use crate::poll::waker::ThreadLocalWaker;

/// The node's own phase wakers.
pub struct Wakers {
    pub input: ThreadLocalWaker,
    pub work: ThreadLocalWaker,
    pub output: ThreadLocalWaker,
}
