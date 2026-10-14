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
use std::rc::Rc;

mod input;
mod output;
mod work;

#[cfg(test)]
mod tests;

pub use input::{LeftInput, RightInput};
pub use output::Output;
pub use work::Work;

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
pub(crate) struct SharedState<
    Left,
    Right,
    Out,
    SignalType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    input: Inputs<Phase<LeftInputType>, Phase<RightInputType>>,
    work: Phase<RoutineType>,
    output: Phase<OutputType>,
    state: NodeState<SignalType>,
}

type Shared<Left, Right, Out, SignalType, RoutineType, LeftInputType, RightInputType, OutputType> =
    Rc<
        RefCell<
            SharedState<
                Left,
                Right,
                Out,
                SignalType,
                RoutineType,
                LeftInputType,
                RightInputType,
                OutputType,
            >,
        >,
    >;

/// Create all four phase pollables sharing state.
pub fn new<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
>(
    input: Inputs<Phase<LeftInputType>, Phase<RightInputType>>,
    work: Phase<RoutineType>,
    output: Phase<OutputType>,
) -> Phases<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
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
