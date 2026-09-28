//! Helpers shared by the sync work nodes.

use crate::error::{Error, ErrorKind};
use crate::thread::ThreadId;
use crate::work::Workable;

/// Work each once; drop those that report Closed. Other errors bubble up.
///
/// Rotates after the round, so the next round starts with the next one: under a bounded input,
/// whoever goes first refills it, and a fixed order would starve the rest.
pub(crate) fn work_each<'params, ThreadIdType: ThreadId>(
    workables: &mut Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,
) -> Result<(), Error> {
    let mut i = 0;
    while i < workables.len() {
        match workables[i].work() {
            Ok(()) => i += 1,
            Err(e) if matches!(e.kind, ErrorKind::Closed) => {
                workables.swap_remove(i);
            }
            Err(e) => return Err(e),
        }
    }
    // The one that went first goes last next round. Moves only the boxes' pointers.
    if workables.len() > 1 {
        workables.rotate_left(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::closed;
    use crate::graph::marker::Connection;
    use std::mem;
    use std::sync::{Arc, Mutex};

    crate::thread_id!(TestThread);

    /// Logs its id each time it is worked; reports Closed if `closes`.
    struct Logged {
        id: usize,
        log: Arc<Mutex<Vec<usize>>>,
        closes: bool,
    }

    impl Connection for Logged {}

    impl Workable for Logged {
        type ThreadId = TestThread;
        fn work(&mut self) -> Result<(), Error> {
            self.log.lock().unwrap().push(self.id);
            if self.closes {
                return Err(closed!());
            }
            Ok(())
        }
    }

    type Workables = Vec<Box<dyn Workable<ThreadId = TestThread>>>;

    /// One workable per `(id, closes)`, all logging into the returned log.
    fn logged(specs: &[(usize, bool)]) -> (Workables, Arc<Mutex<Vec<usize>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let workables = specs
            .iter()
            .map(|&(id, closes)| {
                Box::new(Logged {
                    id,
                    log: log.clone(),
                    closes,
                }) as Box<dyn Workable<ThreadId = TestThread>>
            })
            .collect();
        (workables, log)
    }

    /// Runs one round and returns the order it worked in.
    fn round(workables: &mut Workables, log: &Mutex<Vec<usize>>) -> Vec<usize> {
        work_each(workables).unwrap();
        mem::take(&mut *log.lock().unwrap())
    }

    #[test]
    fn each_round_starts_with_the_next() {
        let (mut workables, log) = logged(&[(0, false), (1, false), (2, false)]);
        assert_eq!(round(&mut workables, &log), vec![0, 1, 2]);
        assert_eq!(round(&mut workables, &log), vec![1, 2, 0]);
        assert_eq!(round(&mut workables, &log), vec![2, 0, 1]);
        assert_eq!(round(&mut workables, &log), vec![0, 1, 2]);
    }

    #[test]
    fn closed_are_dropped_and_the_rest_keep_rotating() {
        let (mut workables, log) = logged(&[(0, false), (1, true), (2, false)]);
        // 1 closes and the last takes its slot, so the round still works everyone once.
        assert_eq!(round(&mut workables, &log), vec![0, 1, 2]);
        assert_eq!(workables.len(), 2);
        assert_eq!(round(&mut workables, &log), vec![2, 0]);
        assert_eq!(round(&mut workables, &log), vec![0, 2]);
    }
}
