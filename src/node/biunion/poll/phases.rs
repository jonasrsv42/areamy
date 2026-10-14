//! Phase grouping structs for biunion poll nodes.

use super::node::{LeftInput, Output, RightInput, Types, Work};
use crate::node::biunion::Inputs;

/// All four phase pollables for a biunion node.
pub struct Phases<T: Types> {
    pub inputs: Inputs<LeftInput<T>, RightInput<T>>,
    pub work: Work<T>,
    pub output: Output<T>,
}
