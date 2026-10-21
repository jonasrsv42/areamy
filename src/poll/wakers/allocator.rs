//! Waker allocator with typestate.
//!
//! Stage 1 — [WakerAllocator]: pre-allocates sync wakers on the main
//! thread. `Send` — crosses to the async thread.
//!
//! Stage 2 — [ThreadLocalWakerAllocator]: created via [WakerAllocator::local].
//! Allocates both sync + local wakers. `!Send`. Consumed by
//! [ThreadLocalWakerAllocator::build] to produce a [Runtime].

use crate::error::Error;
use crate::fatal;
use crate::poll::graph::GraphNode;
use crate::poll::marker::NodeId;
use crate::poll::runtime::{Node, Runtime};
use crate::poll::scheduler::{Producer, ThreadLocalProducer};
use crate::poll::timer::Timer;
use crate::poll::wakers::{sync, thread_local};
use crate::poll::{Context, Pollable};
use crate::thread::ThreadId;
use crate::wake::{self, LocalWaker};

use alloc::vec::Vec;
use core::marker::PhantomData;

/// A node ID paired with a value.
pub struct Slot<T> {
    pub id: NodeId,
    pub value: T,
}

/// Stage 1: sync waker pre-allocation. `Send`.
pub struct WakerAllocator {
    producer: Producer,
    /// Each node's sync waker, by id, for its [Context] in stage 2.
    sync: Vec<core::task::Waker>,
}

impl WakerAllocator {
    pub fn new(producer: Producer) -> Self {
        Self {
            producer,
            sync: Vec::new(),
        }
    }

    /// Pre-allocate a sync waker.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Slot<core::task::Waker> {
        let id = self.sync.len();
        let waker = sync::Waker::task(id, &self.producer);
        self.sync.push(waker.clone());
        Slot { id, value: waker }
    }

    /// Transition to stage 2. Must be called on the async thread.
    pub fn local<ThreadIdType: ThreadId>(
        self,
        local_producer: ThreadLocalProducer,
    ) -> ThreadLocalWakerAllocator<ThreadIdType> {
        let contexts = self
            .sync
            .into_iter()
            .enumerate()
            .map(|(id, sync)| context(id, sync, &local_producer))
            .collect();
        ThreadLocalWakerAllocator {
            producer: self.producer,
            local_producer,
            contexts,
            _thread: PhantomData,
        }
    }
}

/// Stage 2: allocates sync + local wakers. `!Send`.
pub struct ThreadLocalWakerAllocator<ThreadIdType: ThreadId> {
    producer: Producer,
    local_producer: ThreadLocalProducer,
    /// Each node's [Context], by id: the one its builder sees is the one the runtime polls with,
    /// so wakers taken at build `will_wake` the runtime's.
    contexts: Vec<Context>,
    _thread: PhantomData<ThreadIdType>,
}

impl<ThreadIdType: ThreadId> ThreadLocalWakerAllocator<ThreadIdType> {
    /// Allocate a [Context] (sync + local wakers, timer) for the next node.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Slot<Context> {
        let id = self.contexts.len();
        let cx = context(
            id,
            sync::Waker::task(id, &self.producer),
            &self.local_producer,
        );
        self.contexts.push(cx.clone());
        Slot { id, value: cx }
    }

    /// Create a [LocalWaker] for an existing node ID.
    /// Does not allocate a new slot.
    pub fn local_waker(&self, id: NodeId) -> LocalWaker {
        thread_local::Waker::local(id, &self.local_producer)
    }

    /// Finalize allocation and build the [Runtime].
    ///
    /// Each [GraphNode] is placed at its ID index. Every allocated slot
    /// must be filled exactly once. Pairs each with its context and enqueues all
    /// nodes for initial poll.
    pub fn build<'params>(
        self,
        nodes: Vec<GraphNode<'params, ThreadIdType>>,
    ) -> Result<Runtime<'params, ThreadIdType>, Error> {
        let count = self.contexts.len();
        let mut slots: Vec<Option<Box<dyn Pollable<ThreadId = ThreadIdType> + 'params>>> =
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
        for (i, (slot, cx)) in slots.into_iter().zip(self.contexts).enumerate() {
            match slot {
                Some(pollable) => result.push(Node { pollable, cx }),
                None => return Err(fatal!("build: slot {} is empty", i)),
            }
        }

        // Enqueue all nodes for initial poll.
        for i in 0..count {
            self.local_producer.push(i);
        }

        Ok(Runtime { nodes: result })
    }
}

fn context(id: NodeId, sync: core::task::Waker, local_producer: &ThreadLocalProducer) -> Context {
    Context {
        wake: wake::Waker {
            sync,
            local: thread_local::Waker::local(id, local_producer),
        },
        timer: Timer::new(id, local_producer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::marker::Connection;
    use crate::poll::scheduler::PollQueue;

    crate::thread_id!(AllocatorThread);

    struct Idle;

    impl Connection for Idle {}

    impl Pollable for Idle {
        type ThreadId = AllocatorThread;
        fn poll(&mut self, _cx: &mut Context) -> Result<core::task::Poll<()>, Error> {
            Ok(core::task::Poll::Pending)
        }
    }

    fn idle(id: NodeId) -> GraphNode<'static, AllocatorThread> {
        GraphNode {
            id,
            pollable: Box::new(Idle),
        }
    }

    #[test]
    fn the_runtime_polls_with_the_wakers_handed_out() {
        let queue = PollQueue::new();
        let mut stage_one = WakerAllocator::new(queue.producer());
        let early = stage_one.next();
        let (_consumer, local_producer) = queue.local();
        let mut stage_two = stage_one.local::<AllocatorThread>(local_producer);
        let late = stage_two.next();

        let runtime = stage_two
            .build(vec![idle(early.id), idle(late.id)])
            .unwrap();
        assert!(early.value.will_wake(&runtime.nodes[early.id].cx.wake.sync));
        let cx = &runtime.nodes[late.id].cx;
        assert!(late.value.wake.sync.will_wake(&cx.wake.sync));
        assert!(late.value.wake.local.will_wake(&cx.wake.local));
    }
}
