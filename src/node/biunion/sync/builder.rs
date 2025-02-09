use crate::error::Error;
use crate::node::biunion::{
    sync::node::BiunionTrait, sync::node::LeftSource, sync::node::RightSource, sync::Biunion,
    BiunionRoutine,
};
use crate::{
    graph::{Add, Get},
    Message, Origin, Pushable, Workable,
};
use crate::{marker::Multiplicity, ThreadId};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct GraphBuilder<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone,
{
    biunion: BiunionType,
}

pub struct GetInput<MultiplicityType: Multiplicity, BiunionType: BiunionTrait + Sync + Clone> {
    biunion: BiunionType,
    connection: PhantomData<MultiplicityType>,
}

impl<BiunionType: BiunionTrait + Sync + Clone>
    Get<dyn Pushable<Message = Message<BiunionType::Left, BiunionType::Signal>>, LeftSource>
    for GetInput<LeftSource, BiunionType>
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<Message = Message<BiunionType::Left, BiunionType::Signal>>>, Error>
    {
        Get::<dyn Pushable<Message = Message<BiunionType::Left, BiunionType::Signal>>,LeftSource>::get(&self.biunion)
    }
}

impl<BiunionType: BiunionTrait + Sync + Clone>
    Get<dyn Pushable<Message = Message<BiunionType::Right, BiunionType::Signal>>, RightSource>
    for GetInput<RightSource, BiunionType>
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<Message = Message<BiunionType::Right, BiunionType::Signal>>>, Error>
    {
        Get::<dyn Pushable<Message = Message<BiunionType::Right, BiunionType::Signal>>, RightSource>::get(&self.biunion)
    }
}

pub struct Input<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone,
{
    pub left: GetInput<LeftSource, BiunionType>,
    pub right: GetInput<RightSource, BiunionType>,
}

pub struct AddWorker<MultiplicityType: Multiplicity, BiunionType: BiunionTrait + Sync + Clone> {
    biunion: BiunionType,
    connection: PhantomData<MultiplicityType>,
}

impl<BiunionType: BiunionTrait + Sync + Clone>
    Add<dyn Workable<ThreadId = <BiunionType as Workable>::ThreadId>, LeftSource>
    for AddWorker<LeftSource, BiunionType>
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = <BiunionType as Workable>::ThreadId>>,
    ) -> Result<(), Error> {
        Add::<dyn Workable<ThreadId = <BiunionType as Workable>::ThreadId>, LeftSource>::add(
            &mut self.biunion,
            workable,
        )
    }
}

impl<BiunionType: BiunionTrait + Sync + Clone>
    Add<dyn Workable<ThreadId = <BiunionType as Workable>::ThreadId>, RightSource>
    for AddWorker<RightSource, BiunionType>
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = <BiunionType as Workable>::ThreadId>>,
    ) -> Result<(), Error> {
        Add::<dyn Workable<ThreadId = <BiunionType as Workable>::ThreadId>, RightSource>::add(
            &mut self.biunion,
            workable,
        )
    }
}

pub struct Workers<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone,
{
    pub left: AddWorker<LeftSource, BiunionType>,
    pub right: AddWorker<RightSource, BiunionType>,
}

impl<BiunionType> GraphBuilder<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone,
{
    pub fn new(biunion: BiunionType) -> Self {
        GraphBuilder { biunion }
    }

    pub fn input(&self) -> Input<BiunionType> {
        Input {
            left: GetInput {
                biunion: self.biunion.clone(),
                connection: PhantomData,
            },
            right: GetInput {
                biunion: self.biunion.clone(),
                connection: PhantomData,
            },
        }
    }

    pub fn workers(&self) -> Workers<BiunionType> {
        // the line routine is shared mutable `Sync + Clone` so cloning and mutating is OK.
        Workers {
            left: AddWorker {
                biunion: self.biunion.clone(),
                connection: PhantomData,
            },
            right: AddWorker {
                biunion: self.biunion.clone(),
                connection: PhantomData,
            },
        }
    }

    pub fn output<'a>(&'a mut self) -> &'a mut BiunionType {
        &mut self.biunion
    }

    pub fn workable<'a>(&'a self) -> GraphBuilder<BiunionType> {
        self.clone()
    }
}

impl<ThreadIdType, BiunionType> Get<dyn Workable<ThreadId = ThreadIdType>>
    for GraphBuilder<BiunionType>
where
    BiunionType: BiunionTrait<ThreadId = ThreadIdType> + Sync + Clone + 'static,
{
    fn get(&self) -> Result<Box<dyn Workable<ThreadId = ThreadIdType>>, Error> {
        Ok(Box::new(self.biunion.clone()))
    }
}

impl<BiunionType> Add<dyn Pushable<Message = Message<BiunionType::Out, BiunionType::Signal>>>
    for GraphBuilder<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone + 'static,
{
    fn add(
        &mut self,
        pushable: Box<dyn Pushable<Message = Message<BiunionType::Out, BiunionType::Signal>>>,
    ) -> Result<(), Error> {
        Add::add(&mut self.biunion, pushable)
    }
}

pub fn make_biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    GraphBuilder<
        impl BiunionTrait<
                Left = Left,
                Right = Right,
                Out = Out,
                Signal = SignalType,
                BiunionRoutine = RoutineType,
            > + Clone
            + Sync
            + Workable<ThreadId = ThreadIdType>
            + Add<dyn Workable<ThreadId = ThreadIdType>, LeftSource>
            + Add<dyn Workable<ThreadId = ThreadIdType>, RightSource>,
    >,
    Error,
>
where
    ThreadIdType: ThreadId + 'static,
    Left: Clone + Send + Sync + 'static,
    Right: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    RoutineType: 'static + BiunionRoutine<Left, Right, Out>,
{
    let worker = maybe_worker?;

    Ok(GraphBuilder::new(Arc::new(Mutex::new(Biunion::of(worker)))))
}
