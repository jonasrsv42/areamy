//! Waker allocator with typestate.
//!
//! Stage 1 — [WakerAllocator]: pre-allocates sync wakers on the main
//! thread. `Send` — crosses to the async thread.
//!
//! Stage 2 — [ThreadLocalWakerAllocator]: created via [WakerAllocator::local].
//! Allocates both sync + local wakers. `!Send`. Consumed by
//! [ThreadLocalWakerAllocator::build] to produce a [Runtime].

use crate::connect::poll::graph::GraphNode;
use crate::connect::poll::marker::NodeId;
use crate::connect::poll::queue::{Producer, ThreadLocalProducer};
use crate::connect::poll::runtime::{Node, Runtime};
use crate::connect::poll::wakers::sync;
use crate::connect::waker::{self, ThreadLocalWaker};
use crate::error::Error;
use crate::{Pollable, ThreadId, fatal};

use alloc::vec::Vec;
use core::marker::PhantomData;

/// A node ID paired with a value.
pub struct Slot<T> {
    pub id: NodeId,
    pub value: T,
}

/// Stage 1: sync waker pre-allocation. `Send`.
pub struct WakerAllocator {
    count: usize,
    producer: Producer,
}

impl WakerAllocator {
    pub fn new(producer: Producer) -> Self {
        Self { count: 0, producer }
    }

    /// Pre-allocate a sync waker.
    pub fn next(&mut self) -> Slot<core::task::Waker> {
        let id = self.count;
        self.count += 1;
        Slot {
            id,
            value: sync::Waker::new(id, &self.producer),
        }
    }

    /// Transition to stage 2. Must be called on the async thread.
    pub fn local<ThreadIdType: ThreadId>(
        self,
        local_producer: ThreadLocalProducer,
    ) -> ThreadLocalWakerAllocator<ThreadIdType> {
        ThreadLocalWakerAllocator {
            count: self.count,
            producer: self.producer,
            local_producer,
            _thread: PhantomData,
        }
    }
}

/// Stage 2: allocates sync + local wakers. `!Send`.
pub struct ThreadLocalWakerAllocator<ThreadIdType: ThreadId> {
    count: usize,
    producer: Producer,
    local_producer: ThreadLocalProducer,
    _thread: PhantomData<ThreadIdType>,
}

impl<ThreadIdType: ThreadId> ThreadLocalWakerAllocator<ThreadIdType> {
    /// Allocate a waker pair (sync + local) for the next node.
    pub fn next(&mut self) -> Slot<waker::Waker> {
        let id = self.count;
        self.count += 1;
        Slot {
            id,
            value: self.make_waker(id),
        }
    }

    /// Create a [ThreadLocalWaker] for an existing node ID.
    /// Does not allocate a new slot.
    pub fn local_waker(&self, id: NodeId) -> ThreadLocalWaker {
        ThreadLocalWaker::from_producer(id, &self.local_producer)
    }

    /// Finalize allocation and build the [Runtime].
    ///
    /// Each [GraphNode] is placed at its ID index. Every allocated slot
    /// must be filled exactly once. Creates wakers and enqueues all nodes
    /// for initial poll.
    pub fn build(
        self,
        nodes: Vec<GraphNode<ThreadIdType>>,
    ) -> Result<Runtime<ThreadIdType>, Error> {
        let count = self.count;
        let mut slots: Vec<Option<Box<dyn Pollable<ThreadId = ThreadIdType>>>> =
            Vec::with_capacity(count);
        slots.resize_with(count, || None);

        for node in nodes {
            if node.id >= count {
                return Err(fatal!(
                    "build: slot {} out of range (only {} allocated)",
                    node.id,
                    count
                ));
            }
            if slots[node.id].is_some() {
                return Err(fatal!("build: slot {} already occupied", node.id));
            }
            slots[node.id] = Some(node.pollable);
        }

        let mut result = Vec::with_capacity(count);
        for (i, slot) in slots.into_iter().enumerate() {
            match slot {
                Some(pollable) => {
                    let waker = waker::Waker {
                        sync: sync::Waker::new(i, &self.producer),
                        local: ThreadLocalWaker::from_producer(i, &self.local_producer),
                    };
                    result.push(Node { pollable, waker });
                }
                None => return Err(fatal!("build: slot {} is empty", i)),
            }
        }

        // Enqueue all nodes for initial poll.
        for i in 0..count {
            self.local_producer.push(i);
        }

        Ok(Runtime { nodes: result })
    }

    fn make_waker(&self, id: NodeId) -> waker::Waker {
        waker::Waker {
            sync: sync::Waker::new(id, &self.producer),
            local: ThreadLocalWaker::from_producer(id, &self.local_producer),
        }
    }
}
