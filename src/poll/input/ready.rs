//! An input resolved at build time: the node's reserved input slot and what its Input phase reads.

use super::r#async;
use super::sync;
use crate::error::Error;
use crate::poll::edge::{self, Receiver};
use crate::poll::graph::Graph;
use crate::poll::marker::NodeId;
use crate::signal::Origin;
use crate::thread::ThreadId;
use core::num::NonZeroUsize;

/// One input ready to assemble: the slot its Input phase fills, and the edge(s) it reads.
pub(crate) struct Ready<TargetType> {
    pub id: NodeId,
    pub target: TargetType,
}

impl<DataType, SignalType: Origin> sync::Input<DataType, SignalType> {
    /// The cross-thread edge, read in the slot reserved at `.input()`.
    pub(crate) fn ready(self) -> Ready<sync::Receiver<DataType, SignalType>> {
        Ready {
            id: self.slot.id,
            target: self.edge,
        }
    }
}

impl<'params, DataType, SignalType, ThreadIdType>
    r#async::Input<'params, DataType, SignalType, ThreadIdType>
where
    DataType: 'static,
    SignalType: Origin + Clone + Send + core::marker::Sync + 'static,
    ThreadIdType: ThreadId,
{
    /// Build each parent into `graph`, feeding a same-thread edge that wakes slot `id`.
    pub(crate) fn ready(
        self,
        id: NodeId,
        bound: Option<NonZeroUsize>,
        mut graph: Graph<'params, ThreadIdType>,
    ) -> Result<
        (
            Ready<Vec<Receiver<DataType, SignalType>>>,
            Graph<'params, ThreadIdType>,
        ),
        Error,
    > {
        let waker = graph.allocator.local_waker(id);
        let mut edges = Vec::new();
        for parent in self.parents {
            let (sender, receiver) = edge::channel(waker.clone(), bound);
            let built = parent.build(sender, graph.allocator)?;
            graph.allocator = built.allocator;
            graph.nodes.extend(built.nodes);
            edges.push(receiver);
        }
        Ok((Ready { id, target: edges }, graph))
    }
}
