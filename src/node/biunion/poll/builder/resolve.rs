// ============================================================
// Resolve — .parent::<Side>(node) dispatches via Resolve trait
// ============================================================

use super::node::{Allocated, Allocating, BuilderInput, Node};
use super::traits::ResolveParent;
use crate::ThreadId;
use crate::biunion;
use crate::connect::poll::edge::{Async, Deferred, Null, Sync};
use crate::connect::poll::input;
use crate::connect::poll::traits::AsyncParent;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;

// --- Both deferred: parent on Node<Allocating, Deferred, Deferred, Deferred> ---

/// .parent::<Left> on both-deferred → left Async, stays Allocating
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
            input: BuilderInput {
                left: input::r#async::Input {
                    parents: vec![parent],
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// .parent::<Right> on both-deferred → right Async, stays Allocating
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
            input: BuilderInput {
                left: node.input.left,
                right: input::r#async::Input {
                    parents: vec![parent],
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// --- Left Sync, right deferred: .parent::<Right> → Allocated ---

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
            input: BuilderInput {
                left: node.input.left,
                right: input::r#async::Input {
                    parents: vec![parent],
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// --- Right Sync, left deferred: .parent::<Left> → Allocated ---

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
            input: BuilderInput {
                left: input::r#async::Input {
                    parents: vec![parent],
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// --- Right Async, left deferred: .parent::<Left> → Allocated ---

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
            input: BuilderInput {
                left: input::r#async::Input {
                    parents: vec![parent],
                },
                right: node.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// --- Left Async, right deferred: .parent::<Right> → Allocated ---

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
            input: BuilderInput {
                left: node.input.left,
                right: input::r#async::Input {
                    parents: vec![parent],
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// --- Inherent .parent::<S>() method on all states with a Deferred input ---

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            'params,
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'params,
    ) -> S::Resolved
    where
        S: ResolveParent<'params, Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            'params,
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'params,
    ) -> S::Resolved
    where
        S: ResolveParent<'params, Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            'params,
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'params,
    ) -> S::Resolved
    where
        S: ResolveParent<'params, Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            'params,
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'params,
    ) -> S::Resolved
    where
        S: ResolveParent<'params, Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'alloc, 'params, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory<'params>,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            'params,
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'params,
    ) -> S::Resolved
    where
        S: ResolveParent<'params, Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}
