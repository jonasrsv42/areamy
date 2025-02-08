use crate::error::Error;
use crate::node::biunion::{
    sync::node::BiunionTrait, sync::node::LeftSource, sync::node::RightSource, sync::Biunion,
    BiunionRoutine,
};
use crate::{AddPushable, AddWorkable, GetWorkable, Origin, Workable};
use crate::{Connection, GetPushable, ThreadId};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct GraphBuilder<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone,
{
    biunion: BiunionType,
}

pub struct GetInput<ConnectionType: Connection, BiunionType: BiunionTrait + Sync + Clone> {
    biunion: BiunionType,
    connection: PhantomData<ConnectionType>,
}

impl<BiunionType: BiunionTrait + Sync + Clone> GetPushable<LeftSource>
    for GetInput<LeftSource, BiunionType>
{
    type Pushable = <BiunionType as GetPushable<LeftSource>>::Pushable;

    fn get(&self) -> Result<Self::Pushable, Error> {
        GetPushable::<LeftSource>::get(&self.biunion)
    }
}

impl<BiunionType: BiunionTrait + Sync + Clone> GetPushable<RightSource>
    for GetInput<RightSource, BiunionType>
{
    type Pushable = <BiunionType as GetPushable<RightSource>>::Pushable;

    fn get(&self) -> Result<Self::Pushable, Error> {
        GetPushable::<RightSource>::get(&self.biunion)
    }
}

pub struct Input<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone,
{
    pub left: GetInput<LeftSource, BiunionType>,
    pub right: GetInput<RightSource, BiunionType>,
}

pub struct AddWorker<ConnectionType: Connection, BiunionType: BiunionTrait + Sync + Clone> {
    biunion: BiunionType,
    connection: PhantomData<ConnectionType>,
}

impl<BiunionType: BiunionTrait + Sync + Clone> AddWorkable<LeftSource>
    for AddWorker<LeftSource, BiunionType>
{
    type ThreadId = <BiunionType as Workable>::ThreadId;

    fn add<WorkableType: Workable<ThreadId = Self::ThreadId> + 'static>(
        &mut self,
        workable: WorkableType,
    ) -> Result<(), Error> {
        AddWorkable::<LeftSource>::add(&mut self.biunion, workable)
    }
}

impl<BiunionType: BiunionTrait + Sync + Clone> AddWorkable<RightSource>
    for AddWorker<RightSource, BiunionType>
{
    type ThreadId = <BiunionType as Workable>::ThreadId;

    fn add<WorkableType: Workable<ThreadId = Self::ThreadId> + 'static>(
        &mut self,
        workable: WorkableType,
    ) -> Result<(), Error> {
        AddWorkable::<RightSource>::add(&mut self.biunion, workable)
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

impl<BiunionType> AddPushable for GraphBuilder<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone + 'static,
{
    type Message = BiunionType::Message;

    fn add<PushableType: crate::Pushable<Message = Self::Message> + 'static>(
        &mut self,
        pushable: PushableType,
    ) -> Result<(), Error> {
        AddPushable::add(&mut self.biunion, pushable)
    }
}

impl<BiunionType> GetWorkable for GraphBuilder<BiunionType>
where
    BiunionType: BiunionTrait + Sync + Clone + 'static,
{
    type Workable = BiunionType;

    fn get(&self) -> Result<Self::Workable, Error> {
        Ok(self.biunion.clone())
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
            + AddWorkable<LeftSource, ThreadId = ThreadIdType>
            + AddWorkable<RightSource, ThreadId = ThreadIdType>,
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
