//! Async biunion node split into four phase pollables sharing state via
//! `Rc<RefCell<SharedState>>`:
//!
//! - **Input** (one per [InputSide]: left, right): drain its edge → TrySend to the routine as
//!   that side; a refused message is held until the routine wakes that side, and no signal is
//!   read past it
//! - **Work**: poll routine async work (woken by the routine's inputs or IO)
//! - **Output**: drain Next → try_push downstream (at most BUDGET per poll), forward
//!   flush/close/marker signals. A refusal parks it, the message held, until the edge wakes it.
//!
//! State machine (same as line node, `node::poll::NodeState`):
//! - **Running**: Inputs drain, Work polls, Output drains
//! - **Flushing**: Work polls routine until Ready → FlushReady
//! - **FlushReady**: Output drains remaining output, forwards Flush → Running
//! - **MarkerReady**: Work polls routine once (best-effort), Output forwards Marker → Running
//! - **Forwarding**: a refused Flush or Marker waits in Output; delivered → Running
//! - **CloseReady**: Output drains remaining output (an edge still refusing drops it), closes →
//!   Closed
//! - **Closed**: all phases return Closed
//!
//! Flush from either input triggers Flushing immediately (first-flush-wins). The other side
//! reads nothing until Running again, so its held message lands after the Flush, as its
//! queued ones do. A Flush on each side therefore ends two segments, the second empty.
//!
//! Close from either input closes the node once that side has handed over everything queued
//! before the close: a refused message is held until the routine makes room. The other side's
//! held message and everything still queued on it, signals included, are dropped; which side's
//! close is read first can depend on wake order.

use super::phases::Phases;
use crate::edge::rotate::Rotate;
use crate::graph::{Receivable, Schedule};
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::node::poll::NodeState;
use crate::poll::{Budget, Phase};
use crate::signal::Origin;
use crate::thread::ThreadId;
use crate::wake;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

mod input;
mod output;
mod work;

#[cfg(test)]
mod tests;

pub use input::{Input, InputSide};
pub use output::Output;
pub use work::Work;

/// The types one biunion node is built from. Bounds live here once, so phases take a single
/// `T: Types`.
pub trait Types {
    type Left;
    type Right;
    type Out;
    type Signal: Origin + Clone;
    type ThreadId: ThreadId;
    type Routine: BiunionRoutine<Self::Left, Self::Right, Self::Out>;
    type LeftInlet: Receivable<DataType = Self::Left, SignalType = Self::Signal>;
    type RightInlet: Receivable<DataType = Self::Right, SignalType = Self::Signal>;
    /// The node's output: a [Fanout](crate::edge::fanout::Fanout) over several edges (`Out:
    /// Clone`), or a [Single](crate::edge::single::Single) edge taking the move.
    type Output: Rotate<DataType = Self::Out, SignalType = Self::Signal>;
}

/// [Types] from its parts, so [Node::phases] infers them from its arguments.
pub struct Node<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInletType,
    RightInletType,
    OutputType,
>(
    PhantomData<
        fn() -> (
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            RoutineType,
            LeftInletType,
            RightInletType,
            OutputType,
        ),
    >,
);

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInletType,
    RightInletType,
    OutputType,
> Types
    for Node<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInletType,
        RightInletType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInletType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInletType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Rotate<DataType = Out, SignalType = SignalType>,
{
    type Left = Left;
    type Right = Right;
    type Out = Out;
    type Signal = SignalType;
    type ThreadId = ThreadIdType;
    type Routine = RoutineType;
    type LeftInlet = LeftInletType;
    type RightInlet = RightInletType;
    type Output = OutputType;
}

/// Shared state between the two Inputs, Work, and Output phases.
pub(crate) struct SharedState<T: Types> {
    input: Inputs<Phase<T::LeftInlet>, Phase<T::RightInlet>>,
    work: Phase<T::Routine>,
    output: Phase<T::Output>,
    state: NodeState<T::Signal>,
    /// One input budget, shared by both sides.
    budget: Budget,
}

type Shared<T> = Rc<RefCell<SharedState<T>>>;

impl<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInletType,
    RightInletType,
    OutputType,
>
    Node<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInletType,
        RightInletType,
        OutputType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInletType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInletType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Rotate<DataType = Out, SignalType = SignalType>,
{
    /// Create all four phase pollables sharing state. Binds every output edge to `output_wake`
    /// before anything is pushed, so no refusal goes unwoken. Here, not at `Push::connect`: the
    /// node's wakers don't exist yet then.
    pub fn phases(
        input: Inputs<Phase<LeftInletType>, Phase<RightInletType>>,
        work: Phase<RoutineType>,
        mut output: OutputType,
        output_wake: wake::Waker,
        budget: Budget,
    ) -> Phases<Self> {
        let waker = output_wake.local.clone();
        Schedule::schedule(&mut output, output_wake);
        let shared = Rc::new(RefCell::new(SharedState {
            input,
            work,
            output: Phase {
                target: output,
                waker,
            },
            state: NodeState::Running,
            budget,
        }));
        Phases {
            inputs: Inputs {
                left: Input::new(shared.clone()),
                right: Input::new(shared.clone()),
            },
            work: Work::new(shared.clone()),
            output: Output::new(shared),
        }
    }
}
