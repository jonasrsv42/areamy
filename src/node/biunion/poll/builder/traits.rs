//! Dispatch traits for biunion builder methods.
//!
//! [`ResolveParent`] dispatches `up.via(node.at::<Side>())` to resolve
//! left or right input to Async based on the side marker.
//!
//! [`ResolveInput`] dispatches `.input::<Side>()` to resolve
//! left or right input to Sync based on the side marker.
//!
//! [`ResolveOutput`] dispatches `.output()` to resolve
//! the output edge.

use super::node::{BuilderInput, Node};
use crate::node::biunion;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::edge::{Deferred, Edge, Sync};
use crate::poll::input::sync::{Input, Receiver};
use crate::poll::traits::AsyncParent;
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

/// Resolve one input to Async via a parent node.
pub trait ResolveParent<'params, Node, SignalType: Origin, ThreadIdType: ThreadId> {
    type Data;
    type Resolved;
    fn resolve(
        node: Node,
        parent: Box<
            dyn AsyncParent<
                    'params,
                    OutType = Self::Data,
                    SignalType = SignalType,
                    ThreadIdType = ThreadIdType,
                > + 'params,
        >,
    ) -> Self::Resolved;
}

/// Resolve one input to Sync via `.input::<Side>()`.
pub trait ResolveInput<Edge, Node> {
    type Resolved;
    fn resolve(node: Node) -> Self::Resolved;
}

/// Resolve output edge via `.output()`.
pub trait ResolveOutput<Node> {
    type Resolved;
    fn resolve(node: Node) -> Self::Resolved;
}

// ============================================================
// ResolveInput — Sync, on the side still Deferred
// ============================================================

/// Left Deferred → Sync, woken by its reserved slot.
impl<'params, RightEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            'params,
            Deferred,
            RightEdge,
            OutEdge,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Left
where
    RightEdge: Edge,
    OutEdge: Edge,
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        'params,
        Sync,
        RightEdge,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            'params,
            Deferred,
            RightEdge,
            OutEdge,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.left;
        let waker = slot.value.clone();
        Node {
            alloc: Inputs {
                left: (),
                right: node.alloc.right,
            },
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: Input {
                    edge: Receiver::new(waker, node.options.bound.left),
                    slot,
                },
                right: node.input.right,
            },
            output: node.output,
            _phantom: PhantomData,
        }
    }
}

/// Right Deferred → Sync, woken by its reserved slot.
impl<'params, LeftEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveInput<
        Sync,
        Node<
            'params,
            LeftEdge,
            Deferred,
            OutEdge,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    > for biunion::Right
where
    LeftEdge: Edge,
    OutEdge: Edge,
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        'params,
        LeftEdge,
        Sync,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >;
    fn resolve(
        node: Node<
            'params,
            LeftEdge,
            Deferred,
            OutEdge,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
    ) -> Self::Resolved {
        let slot = node.alloc.right;
        let waker = slot.value.clone();
        Node {
            alloc: Inputs {
                left: node.alloc.left,
                right: (),
            },
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: node.input.left,
                right: Input {
                    edge: Receiver::new(waker, node.options.bound.right),
                    slot,
                },
            },
            output: node.output,
            _phantom: PhantomData,
        }
    }
}

// ============================================================
// ResolveOutput — Sync
// ============================================================

/// Resolve output to Sync on any node with Deferred output.
impl<'params, LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveOutput<
        Node<
            'params,
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
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Resolved = Node<
        'params,
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
            'params,
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
            alloc: node.alloc,
            factory: node.factory,
            options: node.options,
            input: node.input,
            output: Default::default(),
            _phantom: PhantomData,
        }
    }
}
