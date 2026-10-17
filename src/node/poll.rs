//! Helpers shared by the poll nodes: one borrowed view per phase, [Feed] for an input, [Drive] for
//! Work and [Emit] for Output, over the node's one [NodeState].

mod drive;
mod emit;
mod feed;
mod state;

pub(crate) use drive::Drive;
pub(crate) use emit::Emit;
pub(crate) use feed::{Feed, Open};
pub(crate) use state::NodeState;
