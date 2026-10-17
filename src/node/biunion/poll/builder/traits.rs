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
use super::types::Types;
use crate::node::biunion;
use crate::node::biunion::Inputs;
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
impl<'params, RightEdge, OutEdge, T>
    ResolveInput<Sync, Node<'params, Deferred, RightEdge, OutEdge, T>> for biunion::Left
where
    RightEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
    T::Left: Send + std::marker::Sync,
{
    type Resolved = Node<'params, Sync, RightEdge, OutEdge, T>;
    fn resolve(node: Node<'params, Deferred, RightEdge, OutEdge, T>) -> Self::Resolved {
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
impl<'params, LeftEdge, OutEdge, T>
    ResolveInput<Sync, Node<'params, LeftEdge, Deferred, OutEdge, T>> for biunion::Right
where
    LeftEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
    T::Right: Send + std::marker::Sync,
{
    type Resolved = Node<'params, LeftEdge, Sync, OutEdge, T>;
    fn resolve(node: Node<'params, LeftEdge, Deferred, OutEdge, T>) -> Self::Resolved {
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
impl<'params, LeftEdge, RightEdge, T> ResolveOutput<Node<'params, LeftEdge, RightEdge, Deferred, T>>
    for Sync
where
    LeftEdge: Edge,
    RightEdge: Edge,
    T: Types<'params>,
    T::Out: Clone + Send + std::marker::Sync,
{
    type Resolved = Node<'params, LeftEdge, RightEdge, Sync, T>;
    fn resolve(node: Node<'params, LeftEdge, RightEdge, Deferred, T>) -> Self::Resolved {
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
