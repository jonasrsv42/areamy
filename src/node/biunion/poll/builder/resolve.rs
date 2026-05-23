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
impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
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
        SignalType,
        ThreadIdType,
    > for biunion::Left
where
    Left: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Left;
    type Resolved = Node<
        Allocating<'a>,
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
        parent: Box<
            dyn AsyncParent<OutType = Left, SignalType = SignalType, ThreadIdType = ThreadIdType>,
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
impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
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
        SignalType,
        ThreadIdType,
    > for biunion::Right
where
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Right;
    type Resolved = Node<
        Allocating<'a>,
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
        parent: Box<
            dyn AsyncParent<OutType = Right, SignalType = SignalType, ThreadIdType = ThreadIdType>,
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

impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
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
        SignalType,
        ThreadIdType,
    > for biunion::Right
where
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Right;
    type Resolved = Node<
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
        parent: Box<
            dyn AsyncParent<OutType = Right, SignalType = SignalType, ThreadIdType = ThreadIdType>,
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

impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
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
        SignalType,
        ThreadIdType,
    > for biunion::Left
where
    Left: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Left;
    type Resolved = Node<
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
        parent: Box<
            dyn AsyncParent<OutType = Left, SignalType = SignalType, ThreadIdType = ThreadIdType>,
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

impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        Node<
            Allocating<'_>,
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Left;
    type Resolved = Node<
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
            Allocating<'_>,
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
            dyn AsyncParent<OutType = Left, SignalType = SignalType, ThreadIdType = ThreadIdType>,
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

impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    ResolveParent<
        Node<
            Allocating<'_>,
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    type Data = Right;
    type Resolved = Node<
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
            Allocating<'_>,
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
            dyn AsyncParent<OutType = Right, SignalType = SignalType, ThreadIdType = ThreadIdType>,
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

impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'static,
    ) -> S::Resolved
    where
        S: ResolveParent<Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'static,
    ) -> S::Resolved
    where
        S: ResolveParent<Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
        Allocating<'a>,
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'static,
    ) -> S::Resolved
    where
        S: ResolveParent<Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
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
    >
where
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'static,
    ) -> S::Resolved
    where
        S: ResolveParent<Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}

impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Node<
        Allocating<'a>,
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
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn parent<S>(
        self,
        parent: impl AsyncParent<
            OutType = S::Data,
            SignalType = SignalType,
            ThreadIdType = ThreadIdType,
        > + 'static,
    ) -> S::Resolved
    where
        S: ResolveParent<Self, SignalType, ThreadIdType>,
    {
        S::resolve(self, Box::new(parent))
    }
}
