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

#[cfg(test)]
mod tests {
    use crate::graph::Pushable;
    use crate::node::line::routine::tests::MockLine;
    use crate::work::Line;
    use crate::{DefaultThread, Message, Pushes, Reads, Schedules, Writes};

    /// `pushes` + `schedules` is an owning edge whose signals follow data; the thread hint
    /// pins a thread-generic pair.
    #[test]
    fn schedules_with_a_thread_hint() {
        let mut up = Line::of(MockLine::new());
        let mut writer = up.writer().unwrap();
        let mut down = Line::of(MockLine::new());
        up.pushes(&down).unwrap();
        down.schedules::<DefaultThread>(up).unwrap();
        let mut reader = down.reader().unwrap();

        writer.push(Message::Data(1)).unwrap();

        // 1 → 2 → 4.
        assert_eq!(reader.read().unwrap(), Message::Data(4));
    }
}
