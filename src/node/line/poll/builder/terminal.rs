//! Terminal async line builder. No linkage — standalone node with sync edges.
//!
//! Input: [SyncBridge] (sync → async, cross-thread).
//! Output: `Vec<Box<dyn Closeable + Send + Sync>>` (async → sync, cross-thread).
//!
//! Added directly to an [AsyncThread] without linking.

use crate::connect::poll::sync_bridge::SyncBridge;
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::Connection;
use crate::node::line::poll::node::AsyncLine;
use crate::node::line::routine::AsyncLineRoutine;
use crate::signal::Origin;
use crate::thread::poll::spawn::Spawnable;
use crate::{Closeable, Linkable, ThreadId};
use std::sync::Arc;
use std::task::Waker;

/// Builder for a terminal [AsyncLine] with sync input and sync output.
///
/// Created on the main thread (`Send`). Wired via [Get]/[Add] for sync
/// connections. Added directly to an [AsyncThread].
#[must_use = "node must be added to AsyncThread via add()"]
pub struct TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    routine: RoutineType,
    input: Arc<SyncBridge<In, SignalType>>,
    outputs: Vec<Box<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>>,
    _thread_id: std::marker::PhantomData<ThreadIdType>,
}

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    pub fn new(routine: RoutineType, waker: Waker) -> Self {
        let input = Arc::new(SyncBridge::new(waker));
        Self {
            routine,
            input,
            outputs: Vec::new(),
            _thread_id: std::marker::PhantomData,
        }
    }

    /// Build the AsyncLine node. Called on the async thread.
    pub fn build(
        self,
    ) -> AsyncLine<
        In,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
        Arc<SyncBridge<In, SignalType>>,
        Vec<Box<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>>,
    > {
        AsyncLine::new(self.routine, self.input, self.outputs)
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType> Connection
    for TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
}

// === Linkable<Terminal>: standalone node, no local edges ===

impl<In, Out, SignalType, ThreadIdType, RoutineType> crate::Linkable<crate::marker::Terminal>
    for TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<In, Out> + 'static,
{
    type Edge = ();
    type Node = Box<dyn crate::Pollable<ThreadId = ThreadIdType>>;

    fn link(self: Box<Self>, _edge: ()) -> Vec<Self::Node> {
        vec![Box::new(self.build())]
    }
}

// === Spawnable: type-erased for AsyncThread ===

impl<In, Out, SignalType, ThreadIdType, RoutineType> Spawnable<ThreadIdType>
    for TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: AsyncLineRoutine<In, Out> + 'static,
{
    fn spawn(self: Box<Self>) -> Vec<Box<dyn crate::Pollable<ThreadId = ThreadIdType>>> {
        self.link(())
    }
}

// === Sync graph connections ===

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync>
    for TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync>, Error>
    {
        Ok(Box::new(self.input.clone()))
    }
}

impl<In, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>
    for TerminalNode<In, Out, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: AsyncLineRoutine<In, Out>,
{
    fn add(
        &mut self,
        connection: Box<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + Sync>,
    ) -> Result<(), Error> {
        self.outputs.push(connection);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::line::routine::tests::AsyncMockLine;
    use crate::{DefaultThread, Message, Pollable, SyncEdge};

    fn noop_waker() -> Waker {
        std::task::Waker::noop().clone()
    }

    #[test]
    fn builder_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<TerminalNode<usize, usize, &str, DefaultThread, AsyncMockLine>>();
    }

    #[test]
    fn build_produces_pollable_node() {
        let builder = TerminalNode::<usize, usize, &str, DefaultThread, _>::new(
            AsyncMockLine::new(),
            noop_waker(),
        );

        let mut node = builder.build();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = Pollable::poll(&mut node, &mut cx).unwrap();
        assert_eq!(node.worker.poll_count, 1);
    }

    #[test]
    fn sync_input_output_works() {
        let builder = TerminalNode::<usize, usize, &str, DefaultThread, _>::new(
            AsyncMockLine::new(),
            noop_waker(),
        );

        // Get sync input edge
        let mut input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
            Get::get(&builder).unwrap();

        let mut node = builder.build();

        // Push into sync input
        input.push(Message::Data(2)).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = Pollable::poll(&mut node, &mut cx).unwrap();
        // Output went nowhere (no output edges added), but routine processed it
    }

    #[test]
    fn sync_output_receives_data() {
        let output = Arc::new(SyncEdge::<usize, &str>::new());

        let mut builder = TerminalNode::<usize, usize, &str, DefaultThread, _>::new(
            AsyncMockLine::new(),
            noop_waker(),
        );

        // Get sync input edge
        let mut input: Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync> =
            Get::get(&builder).unwrap();

        // Add sync output
        Add::add(
            &mut builder,
            Box::new(output.clone())
                as Box<dyn Closeable<DataType = usize, SignalType = &str> + Send + Sync>,
        )
        .unwrap();

        let mut node = builder.build();

        input.push(Message::Data(2)).unwrap();

        let cx_waker = std::task::Waker::noop();
        let mut cx = core::task::Context::from_waker(&cx_waker);

        let _ = Pollable::poll(&mut node, &mut cx).unwrap();

        assert_eq!(output.poll().unwrap(), Some(Message::Data(4)));
    }
}
