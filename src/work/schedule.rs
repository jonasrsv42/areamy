use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::{Connection, Unary};
use crate::work::Workable;

/// [`Schedules`] is the schedule-only edge: `down.schedules(up)?` moves `up` into `down`, which
/// then decides when `up` works. No data flows; pair it with [crate::Pushes] for an owning edge
/// whose signals follow data.
///
/// `down` is single-sided; name a side with [crate::At::at_mut]. The thread is an optional
/// annotation: `down.schedules::<EncoderThread>(up)?`.
///
/// Scheduling can't cycle: `up` is moved.
pub trait Schedules<'params> {
    fn schedules<ThreadIdType>(
        &mut self,
        up: impl Workable<ThreadId = ThreadIdType> + 'params,
    ) -> Result<(), Error>
    where
        Self: Add<dyn Workable<ThreadId = ThreadIdType> + 'params, Unary>;
}

impl<'params, NodeType: Connection> Schedules<'params> for NodeType {
    fn schedules<ThreadIdType>(
        &mut self,
        up: impl Workable<ThreadId = ThreadIdType> + 'params,
    ) -> Result<(), Error>
    where
        Self: Add<dyn Workable<ThreadId = ThreadIdType> + 'params, Unary>,
    {
        Add::add(self, Box::new(up))
    }
}
