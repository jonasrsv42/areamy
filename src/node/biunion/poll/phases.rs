//! Phase grouping structs for biunion poll nodes.

use super::node::{Input, Output, Types, Work};
use crate::node::biunion::{self, Inputs};

/// All four phase pollables for a biunion node.
pub struct Phases<T: Types> {
    pub inputs: Inputs<Input<T, biunion::Left>, Input<T, biunion::Right>>,
    pub work: Work<T>,
    pub output: Output<T>,
}
