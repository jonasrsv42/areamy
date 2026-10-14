//! Async biunion node split into four phase pollables sharing state via
//! `Rc<RefCell<SharedState>>`:
//!
//! - **LeftInput**: drain left edge → Send<Left, biunion::Left> to routine
//! - **RightInput**: drain right edge → Send<Right, biunion::Right> to routine
//! - **Work**: poll routine async work (woken by InputQueue/IO)
//! - **Output**: drain Next → push downstream, forward flush/close/marker signals
//!
//! State machine (same as line node):
//! - **Running**: Inputs drain, Work polls, Output drains
//! - **Flushing**: Work polls routine until Ready → FlushReady
//! - **FlushReady**: Output drains remaining output, forwards Flush → Running
//! - **MarkerReady**: Work polls routine once (best-effort), Output forwards Marker → Running
//! - **Closing**: Work polls routine until Ready → CloseReady
//! - **CloseReady**: Output drains remaining output, closes → Closed
//! - **Closed**: all phases return Closed
//!
//! Flush from either input triggers Flushing immediately (first-flush-wins).

use super::Inputs;
use super::phases::Phases;
use crate::graph::{Closeable, Pushable, Receivable};
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::Phase;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

mod input;
mod output;
mod work;

#[cfg(test)]
mod tests;

pub use input::{LeftInput, RightInput};
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
    type Outlet: Pushable<DataType = Self::Out, SignalType = Self::Signal> + Closeable;
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
    OutletType,
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
            OutletType,
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
    OutletType,
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
        OutletType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInletType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInletType: Receivable<DataType = Right, SignalType = SignalType>,
    OutletType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    type Left = Left;
    type Right = Right;
    type Out = Out;
    type Signal = SignalType;
    type ThreadId = ThreadIdType;
    type Routine = RoutineType;
    type LeftInlet = LeftInletType;
    type RightInlet = RightInletType;
    type Outlet = OutletType;
}

/// Node state machine for managing flush and close lifecycle.
enum NodeState<SignalType> {
    Running,
    Flushing(SignalType),
    FlushReady(SignalType),
    MarkerReady(SignalType),
    CloseReady,
    Closed,
}

/// Shared state between LeftInput, RightInput, Work, and Output phases.
pub(crate) struct SharedState<T: Types> {
    input: Inputs<Phase<T::LeftInlet>, Phase<T::RightInlet>>,
    work: Phase<T::Routine>,
    output: Phase<T::Outlet>,
    state: NodeState<T::Signal>,
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
    OutletType,
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
        OutletType,
    >
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInletType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInletType: Receivable<DataType = Right, SignalType = SignalType>,
    OutletType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    /// Create all four phase pollables sharing state.
    pub fn phases(
        input: Inputs<Phase<LeftInletType>, Phase<RightInletType>>,
        work: Phase<RoutineType>,
        output: Phase<OutletType>,
    ) -> Phases<Self> {
        let shared = Rc::new(RefCell::new(SharedState {
            input,
            work,
            output,
            state: NodeState::Running,
        }));
        Phases {
            inputs: Inputs {
                left: LeftInput::new(shared.clone()),
                right: RightInput::new(shared.clone()),
            },
            work: Work::new(shared.clone()),
            output: Output::new(shared),
        }
    }
}
