//! Graph source traits with lifecycle management.
//!
//! # Shutdown Flow
//!
//! Areamy graphs shut down gracefully through the [`crate::error::ErrorKind::Closed`] error.
//! The shutdown flow works as follows:
//!
//! 1. **Close the source**: Call [`GraphPushSource::close`] (or let a reader drop, which
//!    calls close automatically). This marks the underlying edge as closed.
//!
//! 2. **Buffered data drains**: Any data already in the edge's buffer can still be read.
//!    Only when the buffer is empty will reads return `Closed`.
//!
//! 3. **Workers receive `Closed`**: When a worker tries to read from an empty, closed edge,
//!    it receives [`crate::error::ErrorKind::Closed`].
//!
//! 4. **ThreadStream exits cleanly**: [`crate::ThreadStream`] treats `Closed` as a clean
//!    exit signal (not an error) and returns `Ok(())` from join.
//!
//! # Closed Semantics
//!
//! Once an edge is closed:
//! - **Pushes fail immediately** with `Closed` - no new data can enter
//! - **Reads succeed** while buffered data remains
//! - **Reads return `Closed`** only when the buffer is empty
//!
//! This follows Rust's channel semantics where senders can close, but receivers
//! can still drain remaining messages.
//!
//! # Example
//!
//! ```
//! use areamy::sync::Receiver;
//! use areamy::{Message, error::ErrorKind};
//!
//! // Create a receiver and mint a sender.
//! let rx = Receiver::<i32, usize>::new();
//! let tx = rx.sender();
//!
//! tx.push_back(Message::Data(1)).unwrap();
//! tx.push_back(Message::Data(2)).unwrap();
//!
//! // Close the channel via the sender.
//! tx.close().unwrap();
//!
//! // Pushes now fail with Closed.
//! let err = tx.push_back(Message::Data(3)).unwrap_err();
//! assert!(matches!(err.kind, ErrorKind::Closed));
//!
//! // But buffered data can still be read.
//! assert_eq!(rx.read_front().unwrap(), Message::Data(1));
//! assert_eq!(rx.read_front().unwrap(), Message::Data(2));
//!
//! // Now buffer is empty — reads return Closed.
//! let err = rx.read_front().unwrap_err();
//! assert!(matches!(err.kind, ErrorKind::Closed));
//! ```
//!
//! # Automatic Close on Drop
//!
//! The reader types ([`crate::LineReader`], [`crate::BiunionReader`], [`crate::BifurcationReader`])
//! implement [`Drop`] to call `close()` automatically. This ensures that worker threads
//! are signaled to exit even if you forget to close explicitly.

use crate::Pullable;
use crate::error::Error;

/// Alias for [`crate::Closeable`]. Prefer using `Closeable` directly.
pub use crate::Closeable as GraphPushSource;

/// A [GraphPullSource] is a data source that can be pulled from, typically implementing a Read
/// interface. It implements [crate::Pullable] to be consumed by pullable components in a graph.
/// A prototypical usecase is for a File input reader.
///
/// See the [module documentation](self) for details on shutdown flow.
pub trait GraphPullSource: Pullable {
    /// Close this source, signaling no more data will be produced.
    ///
    /// After closing:
    /// - Further pulls may return [`crate::error::ErrorKind::Closed`]
    /// - The exact semantics depend on the implementation
    fn close(&mut self) -> Result<(), Error>;
}
