// ============================================================
// Resolve — `up.via(node.at::<Side>())` dispatches via Resolve trait
// ============================================================

use super::node::{Allocated, Allocating, BuilderInput, Node};
use super::traits::ResolveParent;
use crate::error::Error;
use crate::graph::{Downstream, Select};
use crate::node::biunion;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::poll::edge::{Async, Deferred, Edge, Null, Sync};
use crate::poll::input;
use crate::poll::traits::{AsyncParent, OwnedPoll};
use crate::signal::Origin;
use crate::thread::ThreadId;
use std::marker::PhantomData;

// --- Both deferred: parent on Node<Allocating, Deferred, Deferred, Deferred> ---

/// `up.via(node.at::<Left>())` on both-deferred → left Async, stays Allocating
impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
        Node<
            'params,
            Allocating<'alloc>,
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
        SignalType,
        ThreadIdType,
    > for biunion::Left
where
    Left: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Left;
    type Resolved = Node<
        'params,
        Allocating<'alloc>,
        Async,
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
            'params,
            Allocating<'alloc>,
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
            alloc: node.alloc,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: input::r#async::Input {
                    parents: vec![parent],
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: PhantomData,
        }
    }
}

/// `up.via(node.at::<Right>())` on both-deferred → right Async, stays Allocating
impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
        Node<
            'params,
            Allocating<'alloc>,
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
        SignalType,
        ThreadIdType,
    > for biunion::Right
where
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Right;
    type Resolved = Node<
        'params,
        Allocating<'alloc>,
        Deferred,
        Async,
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
            'params,
            Allocating<'alloc>,
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
            alloc: node.alloc,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: node.input.left,
                right: input::r#async::Input {
                    parents: vec![parent],
                },
            },
            output: Null::new(),
            _phantom: PhantomData,
        }
    }
}

// --- Left Sync, right deferred: via at::<Right> → Allocated ---

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
        Node<
            'params,
            Allocating<'alloc>,
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
        SignalType,
        ThreadIdType,
    > for biunion::Right
where
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Right;
    type Resolved = Node<
        'params,
        Allocated,
        Sync,
        Async,
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
            'params,
            Allocating<'alloc>,
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
            alloc: Allocated,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: node.input.left,
                right: input::r#async::Input {
                    parents: vec![parent],
                },
            },
            output: Null::new(),
            _phantom: PhantomData,
        }
    }
}

// --- Right Sync, left deferred: via at::<Left> → Allocated ---

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
        Node<
            'params,
            Allocating<'alloc>,
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
        SignalType,
        ThreadIdType,
    > for biunion::Left
where
    Left: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Left;
    type Resolved = Node<
        'params,
        Allocated,
        Async,
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
            'params,
            Allocating<'alloc>,
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
            alloc: Allocated,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: input::r#async::Input {
                    parents: vec![parent],
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: PhantomData,
        }
    }
}

// --- Right Async, left deferred: via at::<Left> → Allocated ---

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
        Node<
            'params,
            Allocating<'alloc>,
            Deferred,
            Async,
            Deferred,
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
    Left: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Left;
    type Resolved = Node<
        'params,
        Allocated,
        Async,
        Async,
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
            'params,
            Allocating<'alloc>,
            Deferred,
            Async,
            Deferred,
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
            alloc: Allocated,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: input::r#async::Input {
                    parents: vec![parent],
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: PhantomData,
        }
    }
}

// --- Left Async, right deferred: via at::<Right> → Allocated ---

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        'params,
        Node<
            'params,
            Allocating<'alloc>,
            Async,
            Deferred,
            Deferred,
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
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Right;
    type Resolved = Node<
        'params,
        Allocated,
        Async,
        Async,
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
            'params,
            Allocating<'alloc>,
            Async,
            Deferred,
            Deferred,
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
            alloc: Allocated,
            factory: node.factory,
            options: node.options,
            input: BuilderInput {
                left: node.input.left,
                right: input::r#async::Input {
                    parents: vec![parent],
                },
            },
            output: Null::new(),
            _phantom: PhantomData,
        }
    }
}

// --- `up.via(node.at::<Side>())` on every state with that side Deferred ---

impl<
    'params,
    AllocState,
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
            AllocState,
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
                AllocState,
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
        > + 'params,
{
    type Out = MultiplicityType::Resolved;

    fn own(self, upstream: UpstreamType) -> Result<Self::Out, Error> {
        Ok(MultiplicityType::resolve(
            self.into_node(),
            Box::new(upstream),
        ))
    }
}
