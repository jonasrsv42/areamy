use super::NodeState;
use crate::error::{Error, ErrorKind};
use crate::fatal;
use crate::graph::marker::Multiplicity;
use crate::graph::{Closed, Inlet, Receivable};
use crate::message::Message;
use crate::node::routine::{Accept, Flush, TrySend};
use crate::poll::{BUDGET, Phase};
use crate::wake::LocalWaker;
use std::ops::ControlFlow::{self, Break, Continue};

/// One input's borrow of a node's state, feeding its routine. On close it sets CloseReady and
/// wakes Work; the node wakes its other phases.
pub(crate) struct Feed<'a, InletType: Inlet, RoutineType> {
    pub(crate) inlet: &'a mut InletType,
    /// This input's own waker.
    pub(crate) waker: &'a LocalWaker,
    pub(crate) work: &'a mut Phase<RoutineType>,
    pub(crate) state: &'a mut NodeState<InletType::SignalType>,
}

impl<InletType, RoutineType> Feed<'_, InletType, RoutineType>
where
    InletType: Receivable + Closed,
    RoutineType: Flush,
{
    /// Feed the routine as side `M`, held message first. Each stop has a waker: empty edge →
    /// upstream push; signal → Output on return to Running; refusal → routine; [BUDGET] → this
    /// input itself; closed → none (CloseReady).
    pub(crate) fn drain<M>(mut self, held: &mut Option<InletType::DataType>) -> Result<(), Error>
    where
        M: Multiplicity,
        RoutineType: TrySend<InletType::DataType, M>,
    {
        if self.retry::<M>(held)?.is_break() {
            return Ok(());
        }
        for _ in 0..BUDGET {
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
            Err(e) if matches!(e.kind, ErrorKind::Closed) => {
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
            Err(e) if matches!(e.kind, ErrorKind::Closed) => Err(fatal!(
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

    /// Fast close: skip flush, skip routine polling; the routine drops with the node, and a
    /// refused message with it. Wake Work so it observes CloseReady and reports closed back to the
    /// poll loop. The node wakes the rest.
    fn close(&mut self) {
        *self.state = NodeState::CloseReady;
        self.work.waker.wake();
    }
}
