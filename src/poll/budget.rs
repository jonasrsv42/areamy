/// Items one poll may move before it yields, re-waking itself so nothing is stranded.
///
/// The answer to life, the universe and fair scheduling: enough to amortise a poll's fixed
/// cost, few enough that no node hogs the thread. Don't panic.
pub const BUDGET: usize = 42;
