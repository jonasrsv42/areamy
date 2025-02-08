use crate::error::Error;
use crate::{
    node::bifurcation::sync::node::BifurcationTrait, sync::Bifurcation, AddPushable, AddWorkable,
    BifurcationRoutine, Connection, GetPushable, GetWorkable, LeftSink, Origin, Pushable,
    RightSink, ThreadId, Workable,
};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait + Sync + Clone,
{
    bifurcation: BifurcationType,
}

pub struct AddOutput<ConnectionType: Connection, BifurcationType: BifurcationTrait + Sync + Clone> {
    bifurcation: BifurcationType,
    connection: PhantomData<ConnectionType>,
}

impl<BifurcationType: BifurcationTrait + Sync + Clone> AddPushable<LeftSink>
    for AddOutput<LeftSink, BifurcationType>
{
    type Message = <BifurcationType as AddPushable<LeftSink>>::Message;

    fn add<PushableType: Pushable<Message = Self::Message> + 'static>(
        &mut self,
        pushable: PushableType,
    ) -> Result<(), Error> {
        AddPushable::<LeftSink>::add(&mut self.bifurcation, pushable)
    }
}

impl<BifurcationType: BifurcationTrait + Sync + Clone> AddPushable<RightSink>
    for AddOutput<RightSink, BifurcationType>
{
    type Message = <BifurcationType as AddPushable<RightSink>>::Message;

    fn add<PushableType: Pushable<Message = Self::Message> + 'static>(
        &mut self,
        pushable: PushableType,
    ) -> Result<(), Error> {
        AddPushable::<RightSink>::add(&mut self.bifurcation, pushable)
    }
}

pub struct Output<BifurcationType>
where
    BifurcationType: BifurcationTrait + Sync + Clone,
{
    pub left: AddOutput<LeftSink, BifurcationType>,
    pub right: AddOutput<RightSink, BifurcationType>,
}

impl<BifurcationType> GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait + Sync + Clone,
{
    pub fn new(bifurcation: BifurcationType) -> Self {
        GraphBuilder { bifurcation }
    }

    pub fn workers<'a>(&'a self) -> BifurcationType {
        self.bifurcation.clone()
    }

    pub fn output(&self) -> Output<BifurcationType> {
        Output {
            left: AddOutput {
                bifurcation: self.bifurcation.clone(),
                connection: PhantomData,
            },
            right: AddOutput {
                bifurcation: self.bifurcation.clone(),
                connection: PhantomData,
            },
        }
    }

    pub fn input<'a>(&'a self) -> &'a GraphBuilder<BifurcationType> {
        self
    }

    pub fn workable(&self) -> Self {
        self.clone()
    }
}

impl<BifurcationType> GetWorkable for GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait + Clone + Sync + 'static,
{
    type Workable = BifurcationType;

    fn get(&self) -> Result<Self::Workable, Error> {
        Ok(self.bifurcation.clone())
    }
}

impl<BifurcationType> GetPushable for GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait + Clone + Sync + 'static,
{
    type Pushable = BifurcationType::Pushable;

    fn get(&self) -> Result<Self::Pushable, Error> {
        BifurcationType::get(&self.bifurcation)
    }
}

impl<BifurcationType> AddWorkable for GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait + Clone + Sync + 'static,
{
    type ThreadId = <BifurcationType as Workable>::ThreadId;
    fn add<WorkableType: Workable<ThreadId = Self::ThreadId> + 'static>(
        &mut self,
        workable: WorkableType,
    ) -> Result<(), Error> {
        <BifurcationType as AddWorkable>::add(&mut self.bifurcation, workable)
    }
}

pub fn make_bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    GraphBuilder<
        impl BifurcationTrait<
                In = In,
                Left = Left,
                Right = Right,
                Signal = SignalType,
                BifurcationRoutine = RoutineType,
            > + AddWorkable<ThreadId = ThreadIdType>
            + Workable<ThreadId = ThreadIdType>
            + Clone
            + Sync,
    >,
    Error,
>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync + 'static,
    Right: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + 'static,
    RoutineType: 'static + BifurcationRoutine<In, Left, Right>,
{
    let worker = maybe_worker?;

    Ok(GraphBuilder::new(Arc::new(Mutex::new(Bifurcation::of(
        worker,
    )))))
}
