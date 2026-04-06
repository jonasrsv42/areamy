//! Biunion node builder with edge kind typestates.
//!
//! Created via [`poll::Thread::biunion`](crate::poll::Thread). Edge kinds
//! are resolved via typestate transitions:
//!
//! - `.left::<Sync>()` → left input becomes Sync (SyncBridge)
//! - `.right::<Sync>()` → right input becomes Sync (SyncBridge)
//! - `.left_parent(node)` → left input becomes Async
//! - `.right_parent(node)` → right input becomes Async
//! - `.typed::<OutEdge>()` → resolve output kind
//!
//! Allocator lifecycle:
//! - `Allocating` — holds `&mut WakerAllocator`, one or both inputs still Deferred
//! - `Allocated` — both inputs resolved, allocator released

use crate::biunion;
use crate::connect::poll::edge::{
    Async, AsyncIn, Deferred, Edge, Null, Sync, SyncBridge, SyncInput,
};
use crate::connect::poll::traits::AsyncParent;
use crate::connect::poll::wakers::WakerAllocator;
use crate::error::Error;
use crate::graph::{Add, Get};
use crate::marker::Connection;
use crate::node::biunion::poll::factory::BiunionRoutineFactory;
use crate::node::biunion::poll::routine::BiunionRoutine;
use crate::signal::Origin;
use crate::{Closeable, ThreadId};
use std::sync::Arc;

/// Builder still holds `&'a mut WakerAllocator` — one or both inputs deferred.
pub struct Allocating<'a>(pub(crate) &'a mut WakerAllocator);

/// Both inputs resolved — allocator borrow released.
pub struct Allocated;

/// Builder input edges grouped together.
pub struct BuilderInput<
    LeftEdge: Edge,
    RightEdge: Edge,
    Left,
    Right,
    SignalType: Origin,
    ThreadIdType: ThreadId,
> {
    pub left: LeftEdge::Input<Left, SignalType, ThreadIdType>,
    pub right: RightEdge::Input<Right, SignalType, ThreadIdType>,
}

/// Biunion node builder.
#[must_use = "node must be consumed (add to thread or pass to another builder)"]
pub struct Node<
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
> where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub(crate) alloc: AllocState,
    pub(crate) factory: FactoryType,
    pub(crate) input: BuilderInput<LeftEdge, RightEdge, Left, Right, SignalType, ThreadIdType>,
    pub(crate) output: OutEdge::Output<Out, SignalType>,
    _phantom: std::marker::PhantomData<(fn() -> Out, fn() -> Left, fn() -> Right, ThreadIdType)>,
}

impl<
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
> Connection
    for Node<
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
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    OutEdge: Edge,
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
}

// ============================================================
// Constructor — both inputs deferred
// ============================================================

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
    SignalType: Origin,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn deferred(factory: FactoryType, alloc: &'a mut WakerAllocator) -> Self {
        Self {
            alloc: Allocating(alloc),
            factory,
            input: BuilderInput {
                left: Default::default(),
                right: Default::default(),
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// Left input transitions
// ============================================================

/// Left Deferred → Sync. Right still deferred — stays Allocating.
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
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    /// Resolve left input to Sync.
    pub fn left(
        self,
    ) -> Node<
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
    > {
        let slot = self.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: self.alloc,
            factory: self.factory,
            input: BuilderInput {
                left: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
                right: self.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// Right input transitions (left already resolved)
// ============================================================

/// Right Deferred → Sync when left is Sync → Allocated
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
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    /// Resolve right input to Sync. Both inputs resolved — Allocated.
    pub fn right(
        self,
    ) -> Node<
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
    > {
        let slot = self.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: Allocated,
            factory: self.factory,
            input: BuilderInput {
                left: self.input.left,
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
// AddParent — resolve input to Async via parent
// ============================================================

use super::traits::AddParent;

/// Both deferred: .parent::<Left>(node) → left becomes Async, stays Allocating
impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType, P> AddParent<biunion::Left, P>
    for Node<
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
    Left: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    P: AsyncParent<Left, SignalType, ThreadIdType> + 'static,
{
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
    fn parent(self, parent: P) -> Self::Resolved {
        Node {
            alloc: self.alloc,
            factory: self.factory,
            input: BuilderInput {
                left: AsyncIn {
                    parents: vec![Box::new(parent)],
                },
                right: self.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Both deferred: .parent::<Right>(node) → right becomes Async, stays Allocating
impl<'a, Left, Right, Out, SignalType, ThreadIdType, FactoryType, P> AddParent<biunion::Right, P>
    for Node<
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
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    P: AsyncParent<Right, SignalType, ThreadIdType> + 'static,
{
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
    fn parent(self, parent: P) -> Self::Resolved {
        Node {
            alloc: self.alloc,
            factory: self.factory,
            input: BuilderInput {
                left: self.input.left,
                right: AsyncIn {
                    parents: vec![Box::new(parent)],
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Left Sync, right deferred: .parent::<Right>(node) → right becomes Async → Allocated
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType, P> AddParent<biunion::Right, P>
    for Node<
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
    >
where
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    P: AsyncParent<Right, SignalType, ThreadIdType> + 'static,
{
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
    fn parent(self, parent: P) -> Self::Resolved {
        Node {
            alloc: Allocated,
            factory: self.factory,
            input: BuilderInput {
                left: self.input.left,
                right: AsyncIn {
                    parents: vec![Box::new(parent)],
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Left Async, right deferred: .parent::<Right>(node) → right becomes Async → Allocated
impl<Left, Right, Out, SignalType, ThreadIdType, FactoryType, P> AddParent<biunion::Right, P>
    for Node<
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
    >
where
    Right: 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
    P: AsyncParent<Right, SignalType, ThreadIdType> + 'static,
{
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
    fn parent(self, parent: P) -> Self::Resolved {
        Node {
            alloc: Allocated,
            factory: self.factory,
            input: BuilderInput {
                left: self.input.left,
                right: AsyncIn {
                    parents: vec![Box::new(parent)],
                },
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Right deferred, left Async: .right() → right becomes Sync → Allocated
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
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    /// Resolve right input to Sync when left is Async. → Allocated.
    pub fn right(
        self,
    ) -> Node<
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
    > {
        let slot = self.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: Allocated,
            factory: self.factory,
            input: BuilderInput {
                left: self.input.left,
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

/// Left deferred after right resolved to Async: .left() → Allocated
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
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    /// Resolve left input to Sync when right is Async. → Allocated.
    pub fn left(
        self,
    ) -> Node<
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
    > {
        let slot = self.alloc.0.next();
        let waker = slot.value.clone();
        Node {
            alloc: Allocated,
            factory: self.factory,
            input: BuilderInput {
                left: SyncInput {
                    edge: Arc::new(SyncBridge::new(waker)),
                    slot,
                },
                right: self.input.right,
            },
            output: Null::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// Output resolution
// ============================================================

/// Resolve output to Sync. Both inputs must be resolved (Allocated).
impl<LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
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
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    pub fn output(
        self,
    ) -> Node<
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
    > {
        Node {
            alloc: Allocated,
            factory: self.factory,
            input: self.input,
            output: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// ============================================================
// Get<dyn Closeable, Left/Right> — Sync inputs with multiplicity
// ============================================================

impl<AllocState, RightEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Get<
        dyn Closeable<DataType = Left, SignalType = SignalType> + Send + std::marker::Sync,
        biunion::Left,
    >
    for Node<
        AllocState,
        Sync,
        RightEdge,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    RightEdge: Edge,
    OutEdge: Edge,
    Left: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Closeable<DataType = Left, SignalType = SignalType> + Send + std::marker::Sync>,
        Error,
    > {
        Ok(Box::new(self.input.left.edge.clone()))
    }
}

impl<AllocState, LeftEdge, OutEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Get<
        dyn Closeable<DataType = Right, SignalType = SignalType> + Send + std::marker::Sync,
        biunion::Right,
    >
    for Node<
        AllocState,
        LeftEdge,
        Sync,
        OutEdge,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    LeftEdge: Edge,
    OutEdge: Edge,
    Right: Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Closeable<DataType = Right, SignalType = SignalType> + Send + std::marker::Sync>,
        Error,
    > {
        Ok(Box::new(self.input.right.edge.clone()))
    }
}

// ============================================================
// Add<dyn Closeable> — Sync output
// ============================================================

impl<AllocState, LeftEdge, RightEdge, Left, Right, Out, SignalType, ThreadIdType, FactoryType>
    Add<dyn Closeable<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync>
    for Node<
        AllocState,
        LeftEdge,
        RightEdge,
        Sync,
        Left,
        Right,
        Out,
        SignalType,
        ThreadIdType,
        FactoryType,
    >
where
    LeftEdge: Edge,
    RightEdge: Edge,
    Out: Clone + Send + std::marker::Sync + 'static,
    SignalType: Origin + Clone + Send + std::marker::Sync + 'static,
    ThreadIdType: ThreadId,
    FactoryType: BiunionRoutineFactory,
    FactoryType::Routine: BiunionRoutine<Left, Right, Out>,
{
    fn add(
        &mut self,
        connection: Box<
            dyn Closeable<DataType = Out, SignalType = SignalType> + Send + std::marker::Sync,
        >,
    ) -> Result<(), Error> {
        self.output.push(connection);
        Ok(())
    }
}
