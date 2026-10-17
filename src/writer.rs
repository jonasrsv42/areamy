//! Graph writers with lifecycle management.
//!
//! # Shutdown Flow
//!
//! Areamy graphs shut down gracefully through the [`crate::error::ErrorKind::Closed`] error.
//! The shutdown flow works as follows:
//!
//! 1. **Close the writer**: Call [`crate::graph::Closeable::close`] (or let a reader drop, which
//!    calls close automatically). This writer is done; the edge closes once every writer into it
//!    is.
//!
//! 2. **Buffered data drains**: Any data already in the edge's buffer can still be read.
//!    Only when the buffer is empty will reads return `Closed`.
//!
//! 3. **Workers receive `Closed`**: When a worker tries to read from an empty, closed edge,
//!    it receives [`crate::error::ErrorKind::Closed`].
//!
//! 4. **ThreadStream exits cleanly**: [`crate::work::ThreadStream`] treats `Closed` as a clean
//!    exit signal (not an error) and returns `Ok(())` from join.
//!
//! # Closed Semantics
//!
//! Each sender is one producer. Closing it:
//! - **Ends that producer**: its pushes fail with `Closed`; other producers keep pushing
//! - **Closes the edge with the last producer**, like Rust's channels on drop
//! - **Keeps what it pushed**: reads succeed while buffered data remains, and return `Closed`
//!   only once the edge is closed and empty
//!
//! Closing the receiver closes the edge for every producer.
//!
//! # Example
//!
//! ```
//! use areamy::sync::Receiver;
//! use areamy::{Message, error::ErrorKind};
//!
//! // Create a receiver and mint two producers.
//! let rx = Receiver::<i32, usize>::new();
//! let mut a = rx.sender();
//! let mut b = rx.sender();
//!
//! a.push_back(Message::Data(1)).unwrap();
//! a.close().unwrap();
//!
//! // `a` is done; `b` still pushes.
//! assert!(matches!(a.push_back(Message::Data(9)).unwrap_err().kind, ErrorKind::Closed));
//! b.push_back(Message::Data(2)).unwrap();
//! b.close().unwrap();
//!
//! // Buffered data still reads; then the edge, closed by its last producer, reports Closed.
//! assert_eq!(rx.read_front().unwrap(), Message::Data(1));
//! assert_eq!(rx.read_front().unwrap(), Message::Data(2));
//! assert!(matches!(rx.read_front().unwrap_err().kind, ErrorKind::Closed));
//! ```
//!
//! # Automatic Close on Drop
//!
//! The I/O types ([`crate::node::line::LineIo`], [`crate::node::biunion::BiunionIo`],
//! [`crate::node::bifurcation::BifurcationIo`])
//! implement [`Drop`] to call `close()` automatically. This ensures that worker threads
//! are signaled to exit even if you forget to close explicitly.

pub mod pull;
pub mod push;
