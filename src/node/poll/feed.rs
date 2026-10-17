use super::NodeState;
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Multiplicity;
use crate::graph::{Inlet, Receivable};
use crate::message::Message;
use crate::node::routine::{Accept, Flush, TrySend};
use crate::poll::Phase;
use crate::wake::LocalWaker;
use std::num::NonZeroUsize;
use std::ops::ControlFlow::{self, Break, Continue};

/// One input's borrow of a node's state, feeding its routine. When the node's last open input
/// closes it sets CloseReady and wakes Work; the node wakes its other phases.
pub(crate) struct Feed<'a, InletType: Inlet, RoutineType> {
    pub(crate) inlet: &'a mut InletType,
    /// This input's own waker.
    pub(crate) waker: &'a LocalWaker,
    pub(crate) open: Open<'a>,
    pub(crate) work: &'a mut Phase<RoutineType>,
    pub(crate) state: &'a mut NodeState<InletType::SignalType>,
    /// Messages taken per poll before yielding.
    pub(crate) budget: NonZeroUsize,
}

/// Which of a node's inputs still feed it, seen from one input. The node ends with its last.
pub(crate) struct Open<'a> {
    /// This input; cleared once its edge closes.
    pub(crate) this: &'a mut bool,
    /// Any other input, so this one closing doesn't end the node.
    pub(crate) others: bool,
}

impl<InletType, RoutineType> Feed<'_, InletType, RoutineType>
where
    InletType: Receivable,
    RoutineType: Flush,
{
    /// Feed the routine as side `M`, held message first. Each stop has a waker: empty edge →
    /// upstream push; signal → Output on return to Running; refusal → routine; budget → this
    /// input itself; closed → none (CloseReady once no input is open).
    pub(crate) fn drain<M>(mut self, held: &mut Option<InletType::DataType>) -> Result<(), Error>
    where
        M: Multiplicity,
        RoutineType: TrySend<InletType::DataType, M>,
    {
        if self.retry::<M>(held)?.is_break() {
            return Ok(());
        }
        // A closed input has nothing more to take; its close counted once.
        if !*self.open.this {
            return Ok(());
        }
        for _ in 0..self.budget.get() {
            if self.recv::<M>(held)?.is_break() {
                return Ok(());
            }
        }
        // Budget spent; the edge may hold more, so wake ourselves.
        self.waker.wake();
        Ok(())
    }

    /// Retry the held message: nothing may overtake it.
    fn retry<M>(&mut self, held: &mut Option<InletType::DataType>) -> Result<ControlFlow<()>, Error>
    where
        M: Multiplicity,
        RoutineType: TrySend<InletType::DataType, M>,
    {
        let Some(data) = held.take() else {
            return Ok(Continue(()));
        };
        let Some(data) = self.offer::<M>(data)? else {
            return Ok(Continue(()));
        };
        self.hold(data, held);
        Ok(Break(()))
    }

    /// Take one message from the edge.
    fn recv<M>(&mut self, held: &mut Option<InletType::DataType>) -> Result<ControlFlow<()>, Error>
    where
        M: Multiplicity,
        RoutineType: TrySend<InletType::DataType, M>,
    {
        match self.inlet.try_recv() {
            Ok(Some(Message::Data(data))) => match self.offer::<M>(data)? {
                None => Ok(Continue(())),
                Some(data) => {
                    self.hold(data, held);
                    Ok(Break(()))
                }
            },
            Ok(Some(Message::Flush(signal))) => {
                self.flushing(signal)?;
                Ok(Break(()))
            }
            Ok(Some(Message::Marker(signal))) => {
                self.marker_ready(signal);
                Ok(Break(()))
            }
            Ok(None) => Ok(Break(())),
            Err(e) if e.is_closed() => {
                self.close();
                Ok(Break(()))
            }
            Err(e) => Err(e),
        }
    }

    /// Hold a refused message until the routine wakes this input, closed edge or not: the refusal
    /// owes that wake, so a Flush queued behind it lands before the close is read.
    fn hold(&mut self, data: InletType::DataType, held: &mut Option<InletType::DataType>) {
        *held = Some(data);
    }

    /// Offer data to the routine as side `M`; the message comes back if it is full. `Closed` from
    /// a routine is fatal: routines handle close, never propagate it.
    fn offer<M>(&mut self, data: InletType::DataType) -> Result<Option<InletType::DataType>, Error>
    where
        M: Multiplicity,
        RoutineType: TrySend<InletType::DataType, M>,
    {
        match TrySend::<InletType::DataType, M>::try_send(&mut self.work.target, data) {
            Ok(Accept::Accepted) => Ok(None),
            Ok(Accept::Full(data)) => Ok(Some(data)),
            Err(e) if e.is_closed() => Err(fatal!(
                "routine returned Closed from try_send — routines must handle close, not propagate it"
            )),
            Err(e) => Err(e),
        }
    }

    fn flushing(&mut self, signal: InletType::SignalType) -> Result<(), Error> {
        self.work.target.flush()?;
        *self.state = NodeState::Flushing(signal);
        // Work must poll routine until Ready to complete the flush.
        self.work.waker.wake();
        Ok(())
    }

    fn marker_ready(&mut self, signal: InletType::SignalType) {
        *self.state = NodeState::MarkerReady(signal);
        // Work gets one poll cycle to let routine emit pending output
        // before Output forwards the marker.
        self.work.waker.wake();
    }

    /// This input is done. The node ends with its last input: then fast close, skipping flush and
    /// routine polling; the routine drops with the node, and a refused message with it. Wake Work
    /// so it observes CloseReady and reports closed back to the poll loop. The node wakes the
    /// rest.
    fn close(&mut self) {
        *self.open.this = false;
        if self.open.others {
            return;
        }
        *self.state = NodeState::CloseReady;
        self.work.waker.wake();
    }
}
