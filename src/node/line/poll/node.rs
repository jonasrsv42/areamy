//! Async line node split into three phase pollables sharing state via
//! `Rc<RefCell<SharedState>>`:
//!
//! - **Input**: drain edge → TrySend to routine; a refused message is held until the routine
//!   wakes Input, and no signal is read past it. The routine wakes Output when it produces.
//! - **Work**: poll routine async work (woken by the routine's input or IO)
//! - **Output**: drain Next → try_push downstream (at most BUDGET per poll), forward
//!   flush/close/marker signals. A refusal parks it, the message held, until the edge wakes it.
//!
//! State machine (`node::poll::NodeState`):
//! - **Running**: Input drains, Work polls, Output drains
//! - **Flushing**: Work polls routine until Ready → FlushReady
//! - **FlushReady**: Output drains remaining output, forwards Flush → Running
//! - **MarkerReady**: Work polls routine once (best-effort drain), Output forwards Marker → Running
//! - **Forwarding**: a refused Flush or Marker waits in Output; delivered → Running
//! - **CloseReady**: Output drains buffered output, closes → Closed
//! - **Closed**: all phases return Closed
//!
//! # Close vs Flush
//!
//! Flush calls `routine.flush()` and polls until Ready — graceful
//! wrap-up. Close is a *fast* path: no flush, no poll. Output drains
//! whatever is already buffered in `routine.next()` and closes the
//! downstream edge; output an edge still refuses is dropped. The routine
//! drops with the node, so its `Drop` is responsible for cancelling any
//! in-flight async work.
//!
//! Input reads the close only after everything queued before it: a message the routine refuses
//! is held until the routine makes room, so a Flush followed by a Close always lands. For a
//! lossless end, Flush before closing.
//!
//! # Marker ordering
//!
//! Markers are synchronization primitives — all data naturally produced
//! before the marker should arrive downstream before it. For sync routines
//! this is guaranteed: TrySend produces output immediately, Output drains it,
//! then forwards the marker.
//!
//! For async routines (e.g. FutureRoutine), the guarantee is **best-effort**:
//! Work is woken for a poll, but Output may forward the marker before that
//! poll runs, so data the routine produces from input accepted before the
//! marker can land after it. If strict ordering is required with async
//! routines, use Flush instead.

use crate::edge::fanout::Fanout;
use crate::graph::{Receivable, Schedule};
use crate::node::line::poll::routine::LineRoutine;
use crate::node::poll::NodeState;
use crate::poll::{self, Budget, Phase};
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

pub use input::Input;
pub use output::Output;
pub use work::Work;

/// The types one line node is built from. Bounds live here once, so phases take a single
/// `T: Types`.
pub trait Types {
    type In;
    /// `Clone` for a fan-out of several edges; a single edge takes the move.
    type Out: Clone;
    type Signal: Origin + Clone;
    type ThreadId: ThreadId;
    type Routine: LineRoutine<Self::In, Self::Out>;
    type Inlet: Receivable<DataType = Self::In, SignalType = Self::Signal>;
    /// One output edge; the node holds a [Fanout] of them.
    type Outlet: poll::Sink<DataType = Self::Out, SignalType = Self::Signal>;
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
    Out: Clone,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InletType: Receivable<DataType = In, SignalType = SignalType>,
    OutletType: poll::Sink<DataType = Out, SignalType = SignalType>,
{
    type In = In;
    type Out = Out;
    type Signal = SignalType;
    type ThreadId = ThreadIdType;
    type Routine = RoutineType;
    type Inlet = InletType;
    type Outlet = OutletType;
}

/// Shared state between Input, Work, and Output phases.
pub(crate) struct SharedState<T: Types> {
    input: Phase<T::Inlet>,
    work: Phase<T::Routine>,
    output: Phase<Fanout<T::Outlet>>,
    state: NodeState<T::Signal>,
    budget: Budget,
}

type Shared<T> = Rc<RefCell<SharedState<T>>>;

/// The three phase pollables of one line node.
pub type Phases<T> = (Input<T>, Work<T>, Output<T>);

impl<In, Out, SignalType, ThreadIdType, RoutineType, InletType, OutletType>
    Node<In, Out, SignalType, ThreadIdType, RoutineType, InletType, OutletType>
where
    Out: Clone,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: LineRoutine<In, Out>,
    InletType: Receivable<DataType = In, SignalType = SignalType>,
    OutletType: poll::Sink<DataType = Out, SignalType = SignalType>,
{
    /// Create Input, Work, and Output phase pollables sharing state. Binds every output edge to
    /// `output_wake` before anything is pushed, so no refusal goes unwoken. Here, not at
    /// `Push::connect`: the node's wakers don't exist yet then.
    pub fn phases(
        input: Phase<InletType>,
        work: Phase<RoutineType>,
        mut output: Fanout<OutletType>,
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
        (
            Input::new(shared.clone()),
            Work::new(shared.clone()),
            Output::new(shared),
        )
    }
}
