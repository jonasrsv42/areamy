//! A thread's CPU clock. Unlike `/proc/<tid>/schedstat`, which lags a running thread by up to a
//! tick, it is current to the read. `Timespec` and the clock id match 64-bit Linux.

use std::fs;
use std::time::Duration;

#[derive(Clone, Copy)]
pub struct CpuClock(i32);

#[repr(C)]
struct Timespec {
    seconds: i64,
    nanoseconds: i64,
}

unsafe extern "C" {
    fn clock_gettime(clock: i32, time: *mut Timespec) -> i32;
}

impl CpuClock {
    pub const SUPPORTED: bool = true;

    /// The clock of this process's thread named `name` (Linux keeps 15 bytes of a name).
    pub fn of(name: &str) -> Self {
        let tasks = fs::read_dir("/proc/self/task").unwrap();
        let mut found = tasks.filter_map(|task| {
            let path = task.unwrap().path();
            let comm = fs::read_to_string(path.join("comm")).ok()?;
            (comm.trim_end() == name).then(|| path.file_name()?.to_str()?.parse::<i32>().ok())?
        });
        let tid = found
            .next()
            .unwrap_or_else(|| panic!("no thread named {name}"));
        assert!(found.next().is_none(), "two threads named {name}");
        // The kernel's MAKE_THREAD_CPUCLOCK(tid, CPUCLOCK_SCHED).
        CpuClock((!tid << 3) | 6)
    }

    pub fn now(self) -> Duration {
        let mut time = Timespec {
            seconds: 0,
            nanoseconds: 0,
        };
        // SAFETY: `time` is a valid, writable timespec for the call.
        let failed = unsafe { clock_gettime(self.0, &mut time) } != 0;
        assert!(!failed, "the measured thread is gone");
        Duration::new(time.seconds as u64, time.nanoseconds as u32)
    }
}
