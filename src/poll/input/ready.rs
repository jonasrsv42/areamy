//! An input resolved at build time: the node's input slot and what its Input phase reads.

use super::r#async;
use super::sync;
use crate::error::Error;
use crate::poll::edge::{self, Receiver};
use crate::poll::graph::{Graph, Reserved, Slot};
use crate::signal::Origin;
use crate::thread::ThreadId;
use core::num::NonZeroUsize;

/// One input ready to assemble: the slot its Input phase fills, and the edge(s) it reads.
pub(crate) struct Ready<TargetType, ThreadIdType: ThreadId> {
    pub slot: Slot<ThreadIdType>,
    pub target: TargetType,
}

impl<DataType, SignalType: Origin, ThreadIdType: ThreadId>
    sync::Input<DataType, SignalType, ThreadIdType>
{
    /// The cross-thread edge, read in the node reserved at mint.
    pub(crate) fn ready(
        self,
        graph: &Graph<'_, ThreadIdType>,
    ) -> Ready<sync::Receiver<DataType, SignalType>, ThreadIdType> {
        Ready {
            slot: graph.claim(self.reserved),
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
    /// Build each parent into `graph`, feeding a same-thread edge that wakes `reserved`.
    pub(crate) fn ready(
        self,
        reserved: Reserved<ThreadIdType>,
        bound: Option<NonZeroUsize>,
        graph: &mut Graph<'params, ThreadIdType>,
    ) -> Result<Ready<Vec<Receiver<DataType, SignalType>>, ThreadIdType>, Error> {
        let slot = graph.claim(reserved);
        let mut edges = Vec::new();
        for parent in self.parents {
            let (sender, receiver) = edge::channel(slot.cx().wake.local.clone(), bound);
            parent.build(sender, graph)?;
            edges.push(receiver);
        }
        Ok(Ready {
            slot,
            target: edges,
        })
    }
}
