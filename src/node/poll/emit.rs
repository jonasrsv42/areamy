use super::NodeState;
use crate::edge::deadlock::{parked, resumed};
use crate::edge::fanout::Rotation;
use crate::edge::rotate::Rotate;
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Multiplicity;
use crate::message::Message;
use crate::node::routine::Next;
use crate::poll::Context;
use crate::poll::Phase;
use crate::wake::LocalWaker;
use std::any::type_name;
use std::mem;
use std::num::NonZeroUsize;

/// One output's borrow of a node's state: drains the routine's output as side `M` downstream,
/// forwards Flush and Marker and wakes the inputs and Work on the return to Running, and closes
/// downstream on CloseReady. Any error closes the node and wakes the inputs and Work to report it.
///
/// Never blocks: an edge refusing parks the Output with the message held in its [Rotate]
/// output, whose edges are bound to the Output's waker; their wake retries it. While parked nothing new is
/// drained, so nothing overtakes the held message. At most `budget` messages leave per poll.
pub(crate) struct Emit<'a, RoutineType, OutputType: Rotate> {
    pub(crate) routine: &'a mut RoutineType,
    pub(crate) output: &'a mut Phase<OutputType>,
    /// The node's inputs, woken once a forwarded signal lets them resume.
    pub(crate) inputs: &'a [&'a LocalWaker],
    /// Work's waker.
    pub(crate) work: &'a LocalWaker,
    pub(crate) state: &'a mut NodeState<OutputType::SignalType>,
    /// Messages delivered per poll before yielding.
    pub(crate) budget: NonZeroUsize,
}

/// How a drain stopped.
enum Drained {
    /// The routine has no more, and nothing is held.
    Done,
    /// An edge refused; its wake retries.
    Parked,
    /// Budget spent; the Output woke itself to go on.
    Yielded,
}

impl<RoutineType, OutputType: Rotate> Emit<'_, RoutineType, OutputType> {
    pub(crate) fn poll<M>(mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error>
    where
        M: Multiplicity,
        RoutineType: Next<OutputType::DataType, M>,
    {
        let result = self.step::<M>();
        if result.is_err() {
            // Output drops on an error: without this, a closed outlet strands the other phases.
            self.close();
        }
        result
    }

    fn step<M>(&mut self) -> Result<core::task::Poll<()>, Error>
    where
        M: Multiplicity,
        RoutineType: Next<OutputType::DataType, M>,
    {
        match &self.state {
            NodeState::Closed => Err(crate::closed!()),
            NodeState::FlushReady(_) | NodeState::MarkerReady(_) => {
                // The signal waits behind the data, in the state.
                if !matches!(self.drain::<M>()?, Drained::Done) {
                    return Ok(core::task::Poll::Pending);
                }
                // Matched just above; the arm only moves the signal out of the state.
                let signal = match mem::replace(self.state, NodeState::Forwarding) {
                    NodeState::FlushReady(signal) => Message::Flush(signal),
                    NodeState::MarkerReady(signal) => Message::Marker(signal),
                    _ => return Err(fatal!("expected FlushReady or MarkerReady state")),
                };
                if self.start(signal)? == Rotation::Complete {
                    self.running();
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::Forwarding => {
                if self.settled()? {
                    self.running();
                }
                Ok(core::task::Poll::Pending)
            }
            NodeState::CloseReady => {
                // Fast close: the routine's buffered output goes out a budget at a time until an
                // edge refuses; that message and everything after it are dropped.
                if matches!(self.drain::<M>()?, Drained::Yielded) {
                    return Ok(core::task::Poll::Pending);
                }
                self.output.target.close()?;
                Err(crate::closed!())
            }
            // Running, Flushing — drain any available output
            NodeState::Running | NodeState::Flushing(_) => {
                let _ = self.drain::<M>()?;
                Ok(core::task::Poll::Pending)
            }
        }
    }

    /// Deliver a held message, then up to `budget` of the routine's side `M` output via [Next].
    fn drain<M>(&mut self) -> Result<Drained, Error>
    where
        M: Multiplicity,
        RoutineType: Next<OutputType::DataType, M>,
    {
        if !self.settled()? {
            return Ok(Drained::Parked);
        }
        for _ in 0..self.budget.get() {
            let Some(out) = Next::<OutputType::DataType, M>::next(self.routine)? else {
                return Ok(Drained::Done);
            };
            if self.start(Message::Data(out))?.is_suspended() {
                return Ok(Drained::Parked);
            }
        }
        // Budget spent; the routine may hold more, so wake ourselves and let others run.
        self.output.waker.wake();
        Ok(Drained::Yielded)
    }

    /// Retry a held message; true once nothing is held.
    fn settled(&mut self) -> Result<bool, Error> {
        let held = self.output.target.suspended();
        let rotation = self.output.target.retry()?;
        self.log(held, rotation);
        Ok(rotation == Rotation::Complete)
    }

    /// Start a rotation; only called with nothing held.
    fn start(
        &mut self,
        message: Message<OutputType::DataType, OutputType::SignalType>,
    ) -> Result<Rotation, Error> {
        let rotation = self.output.target.rotate(message)?;
        self.log(None, rotation);
        Ok(rotation)
    }

    /// Log parking and resuming, as the work side logs blocking: a "parked" with no matching
    /// "resumed" is a stall.
    fn log(&self, held: Option<usize>, rotation: Rotation) {
        let node = type_name::<RoutineType>();
        match (held, rotation) {
            (Some(edge), Rotation::Complete) => resumed(node, edge),
            (Some(edge), Rotation::Suspended(next)) if edge != next => {
                resumed(node, edge);
                parked(node, next);
            }
            (None, Rotation::Suspended(edge)) => parked(node, edge),
            _ => {}
        }
    }

    /// Back to Running: inputs resume draining, and Work replays a routine wake that Drive
    /// swallowed while the signal waited.
    fn running(&mut self) {
        *self.state = NodeState::Running;
        self.wake_inputs();
        self.work.wake();
    }

    fn wake_inputs(&self) {
        for input in self.inputs {
            input.wake();
        }
    }

    /// Close the node and wake its other phases to report Closed.
    fn close(&mut self) {
        *self.state = NodeState::Closed;
        self.wake_inputs();
        self.work.wake();
    }
}
