// ============================================================
// Resolve — `up.via(node.at::<Side>())` dispatches via `ResolveParent`
// ============================================================

use super::node::{BuilderInput, Node};
use super::traits::ResolveParent;
use crate::error::Error;
use crate::graph::{Downstream, Select};
use crate::node::biunion;
use crate::node::biunion::Inputs;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::edge::{Async, Deferred, Edge};
use crate::poll::input;
use crate::poll::traits::{AsyncParent, OwnedPoll};
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

/// `up.via(node.at::<Left>())`: left Deferred → Async, keeping its reserved slot's id.
impl<'params, RightEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
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
        SignalType,
        ThreadIdType,
    > for biunion::Left
where
    RightEdge: Edge,
    OutEdge: Edge,
    Left: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Left;
    type Resolved = Node<
        'params,
        Async,
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
        parent: Box<
            dyn AsyncParent<
                    'params,
                    OutType = Left,
                    SignalType = SignalType,
                    ThreadIdType = ThreadIdType,
                > + 'params,
        >,
    ) -> Self::Resolved {
        Node {
            alloc: Inputs {
                left: node.alloc.left.id,
                right: node.alloc.right,
            },
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

/// `up.via(node.at::<Right>())`: right Deferred → Async, keeping its reserved slot's id.
impl<'params, LeftEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
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
        SignalType,
        ThreadIdType,
    > for biunion::Right
where
    LeftEdge: Edge,
    OutEdge: Edge,
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Right;
    type Resolved = Node<
        'params,
        LeftEdge,
        Async,
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
        parent: Box<
            dyn AsyncParent<
                    'params,
                    OutType = Right,
                    SignalType = SignalType,
                    ThreadIdType = ThreadIdType,
                > + 'params,
        >,
    ) -> Self::Resolved {
        Node {
            alloc: Inputs {
                left: node.alloc.left,
                right: node.alloc.right.id,
            },
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

impl<
    'params,
    LeftEdge,
    RightEdge,
    OutEdge,
    Left,
    Right,
    Out,
    SignalType,
    ThreadIdType,
    FactoryType,
    MultiplicityType,
    UpstreamType,
> Downstream<UpstreamType, MultiplicityType::Data, OwnedPoll>
    for Select<
        Node<
            'params,
            LeftEdge,
            RightEdge,
            OutEdge,
            Left,
            Right,
            Out,
            SignalType,
            ThreadIdType,
            FactoryType,
        >,
        MultiplicityType,
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    MultiplicityType: ResolveParent<
            'params,
            Node<
                'params,
                LeftEdge,
                RightEdge,
                OutEdge,
                Left,
                Right,
                Out,
                SignalType,
                ThreadIdType,
                FactoryType,
            >,
            SignalType,
            ThreadIdType,
        >,
    UpstreamType: AsyncParent<
            'params,
            OutType = MultiplicityType::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
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
