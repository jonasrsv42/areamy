use super::NodeState;
use crate::error::Error;
use crate::fatal;
use crate::graph::marker::Multiplicity;
use crate::graph::{Closeable, Pushable};
use crate::message::Message;
use crate::node::routine::Next;
use crate::poll::Phase;
use crate::poll::waker::{ThreadLocalWaker, Waker};
use std::mem;

/// One output's borrow of a node's state: drains the routine's output as side `M` downstream,
/// forwards Flush and Marker and wakes the inputs on the return to Running, and closes downstream
/// on CloseReady.
pub(crate) struct Emit<'a, RoutineType, OutletType: Pushable> {
    pub(crate) routine: &'a mut RoutineType,
    pub(crate) output: &'a mut Phase<OutletType>,
    /// The node's inputs, woken once a forwarded signal lets them resume.
    pub(crate) inputs: &'a [&'a ThreadLocalWaker],
    pub(crate) state: &'a mut NodeState<OutletType::SignalType>,
}

impl<RoutineType, OutletType> Emit<'_, RoutineType, OutletType>
where
    OutletType: Pushable + Closeable,
{
    pub(crate) fn poll<M>(mut self, _waker: &mut Waker) -> Result<core::task::Poll<()>, Error>
    where
        M: Multiplicity,
        RoutineType: Next<OutletType::DataType, M>,
    {
        match &self.state {
            NodeState::Closed => Err(crate::closed!()),
            NodeState::FlushReady(_) => {
                self.drain::<M>()?;
                let NodeState::FlushReady(signal) = mem::replace(self.state, NodeState::Running)
                else {
                    return Err(fatal!("expected FlushReady state"));
                };
                self.output.target.push(Message::Flush(signal))?;
                // Flush forwarded, back to Running. Inputs can resume draining.
                self.wake_inputs();
                Ok(core::task::Poll::Pending)
            }
            NodeState::MarkerReady(_) => {
                self.drain::<M>()?;
                let NodeState::MarkerReady(signal) = mem::replace(self.state, NodeState::Running)
                else {
                    return Err(fatal!("expected MarkerReady state"));
                };
                self.output.target.push(Message::Marker(signal))?;
                // Marker forwarded, back to Running. Inputs can resume draining.
                self.wake_inputs();
                Ok(core::task::Poll::Pending)
            }
            NodeState::CloseReady => {
                self.drain::<M>()?;
                self.output.target.close()?;
                *self.state = NodeState::Closed;
                Err(crate::closed!())
            }
            // Running, Flushing — drain any available output
            NodeState::Running | NodeState::Flushing(_) => {
                self.drain::<M>()?;
                Ok(core::task::Poll::Pending)
            }
        }
    }

    /// Drain the routine's side `M` output via [Next] and push downstream.
    fn drain<M>(&mut self) -> Result<(), Error>
    where
        M: Multiplicity,
        RoutineType: Next<OutletType::DataType, M>,
    {
        while let Some(out) = Next::<OutletType::DataType, M>::next(self.routine)? {
            self.output.target.push(Message::Data(out))?;
        }
        Ok(())
    }

    fn wake_inputs(&self) {
        for input in self.inputs {
            input.wake();
        }
    }
}
