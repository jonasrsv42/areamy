//! Helpers shared by the sync work nodes.

use crate::error::{Error, ErrorKind};
use crate::{ThreadId, Workable};

/// Work each once; drop those that report Closed. Other errors bubble up.
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
    Ok(())
}
