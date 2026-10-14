//! Phase grouping structs for biunion poll nodes.

use super::Inputs;
use super::node::{LeftInput, Output, RightInput, Types, Work};

/// All four phase pollables for a biunion node.
pub struct Phases<T: Types> {
    pub inputs: Inputs<LeftInput<T>, RightInput<T>>,
    pub work: Work<T>,
    pub output: Output<T>,
}
