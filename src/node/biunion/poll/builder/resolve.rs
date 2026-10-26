// ============================================================
// Resolve — `up.via(node.at::<Side>())` dispatches via `ResolveParent`
// ============================================================

use super::node::{BuilderInput, Node};
use super::traits::ResolveParent;
use super::types::Types;
use crate::error::Error;
use crate::graph::{Downstream, Select};
use crate::node::biunion;
use crate::poll::edge::{Async, Deferred, Edge};
use crate::poll::input;
use crate::poll::traits::{AsyncParent, OwnedPoll};
use core::marker::PhantomData;

/// `up.via(node.at::<Left>())`: left Deferred → Async, keeping its reserved node.
impl<'params, RightEdge, OutEdge, T>
    ResolveParent<'params, Node<'params, Deferred, RightEdge, OutEdge, T>, T::Signal, T::ThreadId>
    for biunion::Left
where
    RightEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
{
    type Data = T::Left;
    type Resolved = Node<'params, Async, RightEdge, OutEdge, T>;
    fn resolve(
        node: Node<'params, Deferred, RightEdge, OutEdge, T>,
        parent: Box<
            dyn AsyncParent<
                    'params,
                    OutType = T::Left,
                    SignalType = T::Signal,
                    ThreadIdType = T::ThreadId,
                > + 'params,
        >,
    ) -> Self::Resolved {
        Node {
            reservation: node.reservation,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: input::r#async::Input {
                    parents: vec![parent],
                },
                right: node.input.right,
            },
            output: node.output,
            _phantom: PhantomData,
        }
    }
}

/// `up.via(node.at::<Right>())`: right Deferred → Async, keeping its reserved node.
impl<'params, LeftEdge, OutEdge, T>
    ResolveParent<'params, Node<'params, LeftEdge, Deferred, OutEdge, T>, T::Signal, T::ThreadId>
    for biunion::Right
where
    LeftEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
{
    type Data = T::Right;
    type Resolved = Node<'params, LeftEdge, Async, OutEdge, T>;
    fn resolve(
        node: Node<'params, LeftEdge, Deferred, OutEdge, T>,
        parent: Box<
            dyn AsyncParent<
                    'params,
                    OutType = T::Right,
                    SignalType = T::Signal,
                    ThreadIdType = T::ThreadId,
                > + 'params,
        >,
    ) -> Self::Resolved {
        Node {
            reservation: node.reservation,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: node.input.left,
                right: input::r#async::Input {
                    parents: vec![parent],
                },
            },
            output: node.output,
            _phantom: PhantomData,
        }
    }
}

// --- `up.via(node.at::<Side>())` on every state with that side Deferred ---

impl<'params, LeftEdge, RightEdge, OutEdge, T, MultiplicityType, UpstreamType>
    Downstream<UpstreamType, MultiplicityType::Data, OwnedPoll>
    for Select<Node<'params, LeftEdge, RightEdge, OutEdge, T>, MultiplicityType>
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    T: Types<'params>,
    MultiplicityType: ResolveParent<
            'params,
            Node<'params, LeftEdge, RightEdge, OutEdge, T>,
            T::Signal,
            T::ThreadId,
        >,
    UpstreamType: AsyncParent<
            'params,
            OutType = MultiplicityType::Data,
            SignalType = T::Signal,
            ThreadIdType = T::ThreadId,
        >,
{
    type Out = MultiplicityType::Resolved;

    fn own(self, upstream: UpstreamType) -> Result<Self::Out, Error> {
        Ok(MultiplicityType::resolve(
            self.into_node(),
            Box::new(upstream),
        ))
    }
}
