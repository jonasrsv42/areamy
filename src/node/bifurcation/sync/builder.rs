use crate::error::Error;
use crate::{
    graph::{Add, Get},
    marker::Multiplicity,
    node::bifurcation::sync::node::BifurcationTrait,
    sync::Bifurcation,
    BifurcationRoutine, LeftSink, Message, Origin, Pushable, RightSink, ThreadId, Workable,
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

pub struct AddOutput<
    MultiplicityType: Multiplicity,
    BifurcationType: BifurcationTrait + Sync + Clone,
> {
    bifurcation: BifurcationType,
    connection: PhantomData<MultiplicityType>,
}

impl<BifurcationType: BifurcationTrait + Sync + Clone>
    Add<dyn Pushable<Message = Message<BifurcationType::Left, BifurcationType::Signal>>, LeftSink>
    for AddOutput<LeftSink, BifurcationType>
{
    fn add(
        &mut self,
        pushable: Box<
            dyn Pushable<Message = Message<BifurcationType::Left, BifurcationType::Signal>>,
        >,
    ) -> Result<(), Error> {
        Add::<
            dyn Pushable<Message = Message<BifurcationType::Left, BifurcationType::Signal>>,
            LeftSink,
        >::add(&mut self.bifurcation, pushable)
    }
}

impl<BifurcationType: BifurcationTrait + Sync + Clone>
    Add<dyn Pushable<Message = Message<BifurcationType::Right, BifurcationType::Signal>>, RightSink>
    for AddOutput<RightSink, BifurcationType>
{
    fn add(
        &mut self,
        pushable: Box<
            dyn Pushable<Message = Message<BifurcationType::Right, BifurcationType::Signal>>,
        >,
    ) -> Result<(), Error> {
        Add::<
            dyn Pushable<Message = Message<BifurcationType::Right, BifurcationType::Signal>>,
            RightSink,
        >::add(&mut self.bifurcation, pushable)
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

impl<BifurcationType>
    Get<dyn Pushable<Message = Message<BifurcationType::In, BifurcationType::Signal>>>
    for GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait + Clone + Sync + 'static,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Pushable<Message = Message<BifurcationType::In, BifurcationType::Signal>>>,
        Error,
    > {
        BifurcationType::get(&self.bifurcation)
    }
}

impl<ThreadIdType, BifurcationType> Get<dyn Workable<ThreadId = ThreadIdType>>
    for GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait<ThreadId = ThreadIdType> + Clone + Sync + 'static,
{
    fn get(&self) -> Result<Box<dyn Workable<ThreadId = ThreadIdType>>, Error> {
        Ok(Box::new(self.bifurcation.clone()))
    }
}

impl<BifurcationType> Add<dyn Workable<ThreadId = <BifurcationType as Workable>::ThreadId>>
    for GraphBuilder<BifurcationType>
where
    BifurcationType: BifurcationTrait + Clone + Sync + 'static,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = <BifurcationType as Workable>::ThreadId>>,
    ) -> Result<(), Error> {
        <BifurcationType as Add<dyn Workable<ThreadId = <BifurcationType as Workable>::ThreadId>>>::add(&mut self.bifurcation, workable)
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
            > + Add<dyn Workable<ThreadId = ThreadIdType>>
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
