//! Poll-only bench helpers: the audio frame payload and a synchronous hop.

use areamy::error::Error;
use areamy::poll::{Context, LineWakers};
use areamy::wake::LocalWaker;
use areamy::{Accept, Flush, Next, Poll, TrySend, poll};
use std::collections::VecDeque;

/// Samples in one 20 ms frame of 16 kHz mono audio.
pub const FRAME_SAMPLES: usize = 320;

pub type AudioFrame = Vec<i16>;

pub fn audio_frame() -> AudioFrame {
    vec![0; FRAME_SAMPLES]
}

/// A synchronous stage on a poll thread, forwarding each frame unchanged. Wakes Output after
/// every send, as downstream's sync-to-poll bridge does, so the bench sees the same wakes.
pub struct SyncHop<T> {
    output: VecDeque<T>,
    output_waker: LocalWaker,
    flushing: bool,
}

impl<T> SyncHop<T> {
    pub fn new(wakers: LineWakers) -> Self {
        Self {
            output: VecDeque::new(),
            output_waker: wakers.output,
            flushing: false,
        }
    }
}

impl<T> TrySend<T> for SyncHop<T> {
    fn try_send(&mut self, message: T) -> Result<Accept<T>, Error> {
        self.output.push_back(message);
        self.output_waker.wake();
        Ok(Accept::Accepted)
    }
}

impl<T> Next<T> for SyncHop<T> {
    fn next(&mut self) -> Result<Option<T>, Error> {
        Ok(self.output.pop_front())
    }
}

impl<T> Flush for SyncHop<T> {
    fn flush(&mut self) -> Result<(), Error> {
        self.flushing = true;
        self.output_waker.wake();
        Ok(())
    }
}

/// Synchronous: all work happens in `try_send`, so a flush completes on the next poll.
impl<T> Poll for SyncHop<T> {
    fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
        if self.flushing {
            self.flushing = false;
            Ok(core::task::Poll::Ready(()))
        } else {
            Ok(core::task::Poll::Pending)
        }
    }
}

impl<T> poll::LineRoutine<T, T> for SyncHop<T> {}
