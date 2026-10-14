//! Phase grouping structs for biunion poll nodes.

use super::Inputs;
use super::node::{LeftInput, Output, RightInput, Work};
use crate::graph::{Closeable, Pushable, Receivable};
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;
use crate::thread::ThreadId;

/// All four phase pollables for a biunion node.
pub struct Phases<
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    RoutineType,
    LeftInputType,
    RightInputType,
    OutputType,
> where
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
    LeftInputType: Receivable<DataType = Left, SignalType = SignalType>,
    RightInputType: Receivable<DataType = Right, SignalType = SignalType>,
    OutputType: Pushable<DataType = Out, SignalType = SignalType> + Closeable,
{
    pub inputs: Inputs<
        LeftInput<
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            RoutineType,
            LeftInputType,
            RightInputType,
            OutputType,
        >,
        RightInput<
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            RoutineType,
            LeftInputType,
            RightInputType,
            OutputType,
        >,
    >,
    pub work: Work<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
    pub output: Output<
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        LeftInputType,
        RightInputType,
        OutputType,
    >,
}
