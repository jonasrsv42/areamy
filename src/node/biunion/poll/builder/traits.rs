//! Dispatch traits for biunion builder methods.
//!
//! [`ResolveParent`] dispatches `.parent::<Side>(node)` to resolve
//! left or right input to Async based on the side marker.
//!
//! [`ResolveInput`] dispatches `.input::<Side, Edge>()` to resolve
//! left or right input to Sync based on the side marker.
//!
//! [`ResolveOutput`] dispatches `.output::<Edge>()` to resolve
//! the output edge.

use super::node::{Allocated, Allocating, BuilderInput, Node};
use crate::ThreadId;
use crate::biunion;
use crate::connect::poll::edge::{Deferred, Edge, Null, Sync, SyncBridge, SyncInput};
use crate::connect::poll::traits::AsyncParent;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;
use std::sync::Arc;

/// Resolve one input to Async via a parent node.
pub trait ResolveParent<Node, SignalType: Origin, ThreadIdType: ThreadId> {
    type Data;
    type Resolved;
    fn resolve(
        node: Node,
        parent: Box<
            dyn AsyncParent<
                    OutType = Self::Data,
                    SignalType = SignalType,
                    ThreadIdType = ThreadIdType,
                >,
        >,
    ) -> Self::Resolved;
}

/// Resolve one input to Sync via `.input::<Side, Edge>()`.
pub trait ResolveInput<Edge, Node> {
    type Resolved;
    fn resolve(node: Node) -> Self::Resolved;
}

/// Resolve output edge via `.output::<Edge>()`.
pub trait ResolveOutput<Node> {
    type Resolved;
    fn resolve(node: Node) -> Self::Resolved;
}

// ============================================================
// ResolveInput — Left + Sync
// ============================================================

/// Left Deferred → Sync, right still Deferred → stays Allocating.
impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            Allocating<'a>,
            Deferred,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Left
where
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        Allocating<'a>,
        Sync,
        Deferred,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            Allocating<'a>,
            Deferred,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: node.alloc,
            factory: node.factory,
            input: BuilderInput {
                left: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Left Deferred → Sync, right already Sync → Allocated.
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            Allocating<'_>,
            Deferred,
            Sync,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Left
where
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        Allocated,
        Sync,
        Sync,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            Allocating<'_>,
            Deferred,
            Sync,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: Allocated,
            factory: node.factory,
            input: BuilderInput {
                left: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Left Deferred → Sync, right already Async → Allocated.
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            Allocating<'_>,
            Deferred,
            crate::connect::poll::edge::Async,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Left
where
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        Allocated,
        Sync,
        crate::connect::poll::edge::Async,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            Allocating<'_>,
            Deferred,
            crate::connect::poll::edge::Async,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: Allocated,
            factory: node.factory,
            input: BuilderInput {
                left: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// ResolveInput — Right + Sync
// ============================================================

/// Right Deferred → Sync, left still Deferred → stays Allocating.
impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            Allocating<'a>,
            Deferred,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Right
where
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        Allocating<'a>,
        Deferred,
        Sync,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            Allocating<'a>,
            Deferred,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: node.alloc,
            factory: node.factory,
            input: BuilderInput {
                left: node.input.left,
                right: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Right Deferred → Sync, left already Sync → Allocated.
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            Allocating<'_>,
            Sync,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Right
where
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        Allocated,
        Sync,
        Sync,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            Allocating<'_>,
            Sync,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: Allocated,
            factory: node.factory,
            input: BuilderInput {
                left: node.input.left,
                right: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Right Deferred → Sync, left already Async → Allocated.
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            Allocating<'_>,
            crate::connect::poll::edge::Async,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Right
where
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        Allocated,
        crate::connect::poll::edge::Async,
        Sync,
        Deferred,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            Allocating<'_>,
            crate::connect::poll::edge::Async,
            Deferred,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: Allocated,
            factory: node.factory,
            input: BuilderInput {
                left: node.input.left,
                right: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// ResolveOutput — Sync
// ============================================================

/// Resolve output to Sync on any Allocated node with Deferred output.
impl<LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveOutput<
        Node<
            Allocated,
            LeftEdge,
            RightEdge,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for Sync
where
    LeftEdge: Edge,
    RightEdge: Edge,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        Allocated,
        LeftEdge,
        RightEdge,
        Sync,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            Allocated,
            LeftEdge,
            RightEdge,
            Deferred,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        Node {
            alloc: Allocated,
            factory: node.factory,
            input: node.input,
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}
