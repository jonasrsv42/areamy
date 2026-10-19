//! No CPU clock off 64-bit Linux: benches skip the cpu metric.

use std::time::Duration;

/// Never made: benches check `SUPPORTED` first.
#[derive(Clone, Copy)]
pub struct CpuClock;

impl CpuClock {
    pub const SUPPORTED: bool = false;

    pub fn of(_name: &str) -> Self {
        unreachable!("benches skip the cpu metric where it is unsupported")
    }

    pub fn now(self) -> Duration {
        unreachable!("benches skip the cpu metric where it is unsupported")
    }
}
