//! Async line node split into three phase pollables sharing state via
//! `Rc<RefCell<SharedState>>`:
//!
//! - **Input**: drain edge → Send to routine. Routine wakes Output via OutputProducer.
//! - **Work**: poll routine async work (woken by InputQueue/IO)
//! - **Output**: drain Next → push downstream, forward flush/close/marker signals
//!
//! State machine:
//! - **Running**: Input drains, Work polls, Output drains
//! - **Flushing**: Work polls routine until Ready → FlushReady
//! - **FlushReady**: Output drains remaining output, forwards Flush → Running
//! - **MarkerReady**: Work polls routine once (best-effort drain), Output forwards Marker → Running
//! - **CloseReady**: Output drains buffered output, closes → Closed
//! - **Closed**: all phases return Closed
//!
//! # Close vs Flush
//!
//! Flush calls `routine.flush()` and polls until Ready — graceful
//! wrap-up. Close is a *fast* path: no flush, no poll. Output drains
//! whatever is already buffered in `routine.next()` and closes the
//! downstream edge; the routine drops with the node, so its `Drop`
//! is responsible for cancelling any in-flight async work.
//!
//! # Marker ordering
//!
//! Markers are synchronization primitives — all data naturally produced
//! before the marker should arrive downstream before it. For sync routines
//! this is guaranteed: Send produces output immediately, Output drains it,
//! then forwards the marker.
//!
//! For async routines (e.g. FutureRoutine), the guarantee is **best-effort**.
//! Work gets one poll cycle to let the routine emit pending output before
//! Output forwards the marker. Data produced by the routine *after* that
//! poll cycle (e.g. from an in-flight future) may arrive after the marker.
//! If strict ordering is required with async routines, use Flush instead.

use crate::graph::{Closeable, Pushable, Receivable};
use crate::node::line::poll::routine::LineRoutine;
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

pub use input::Input;
pub use output::Output;
pub use work::Work;

/// The types one line node is built from. Bounds live here once, so phases take a single
/// `T: Types`.
pub trait Types {
    type In;
    type Out;
    type Signal: Origin + Clone;
    type ThreadId: ThreadId;
    type Routine: LineRoutine<Self::In, Self::Out>;
    type Inlet: Receivable<DataType = Self::In, SignalType = Self::Signal>;
    type Outlet: Pushable<DataType = Self::Out, SignalType = Self::Signal> + Closeable;
}

/// [Types] from its parts, so [Node::phases] infers them from its arguments.
pub struct Node<In, Out, SignalType, ThreadIdType, RoutineType, InletType, OutletType>(
    PhantomData<
        fn() -> (
            In,
            Out,
            SignalType,
            ThreadIdType,
            RoutineType,
            InletType,
            OutletType,
        ),
    >,
);

impl<In, Out, SignalType, ThreadIdType, RoutineType, InletType, OutletType> Types
    for Node<In, Out, SignalType, ThreadIdType, RoutineType, InletType, OutletType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InletType: Receivable<DataType = In, SignalType = SignalType>,
    OutletType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    type In = In;
    type Out = Out;
    type Signal = SignalType;
    type ThreadId = ThreadIdType;
    type Routine = RoutineType;
    type Inlet = InletType;
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

/// Shared state between Input, Work, and Output phases.
pub(crate) struct SharedState<T: Types> {
    input: Phase<T::Inlet>,
    work: Phase<T::Routine>,
    output: Phase<T::Outlet>,
    state: NodeState<T::Signal>,
}

type Shared<T> = Rc<RefCell<SharedState<T>>>;

/// The three phase pollables of one line node.
pub type Phases<T> = (Input<T>, Work<T>, Output<T>);

impl<In, Out, SignalType, ThreadIdType, RoutineType, InletType, OutletType>
    Node<In, Out, SignalType, ThreadIdType, RoutineType, InletType, OutletType>
where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InletType: Receivable<DataType = In, SignalType = SignalType>,
    OutletType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    /// Create Input, Work, and Output phase pollables sharing state.
    pub fn phases(
        input: Phase<InletType>,
        work: Phase<RoutineType>,
        output: Phase<OutletType>,
    ) -> Phases<Self> {
        let shared = Rc::new(RefCell::new(SharedState {
            input,
            work,
            output,
            state: NodeState::Running,
        }));
        (
            Input::new(shared.clone()),
            Work::new(shared.clone()),
            Output::new(shared),
        )
    }
}
