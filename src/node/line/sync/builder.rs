use crate::error::Error;
use crate::node::line::sync::bridge::Bridge;
use crate::node::line::sync::node::LineTrait;
use crate::{
    graph::{Add, Get},
    sync::Line,
    LineRoutine, Message, Origin, Pullable, Pushable, ThreadId, Workable,
};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct GraphBuilder<LineType>
where
    LineType: LineTrait + Sync + Clone,
{
    line: LineType,
}

impl<LineType> GraphBuilder<LineType>
where
    LineType: LineTrait + Sync + Clone,
{
    pub fn new(line: LineType) -> Self {
        GraphBuilder { line }
    }

    pub fn workers<'a>(&'a mut self) -> &'a mut LineType {
        &mut self.line
    }

    pub fn input<'a>(&'a mut self) -> &'a mut LineType {
        &mut self.line
    }

    pub fn output<'a>(&'a mut self) -> &'a mut LineType {
        &mut self.line
    }

    pub fn workable(&self) -> GraphBuilder<LineType> {
        self.clone()
    }
}

impl<LineType> Get<dyn Pushable<Message = Message<LineType::In, LineType::Signal>>>
    for GraphBuilder<LineType>
where
    LineType: LineTrait + Sync + Clone + 'static,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<Message = Message<LineType::In, LineType::Signal>>>, Error> {
        Get::get(&self.line)
    }
}

impl<ThreadIdType, LineType> Get<dyn Workable<ThreadId = ThreadIdType>> for GraphBuilder<LineType>
where
    LineType: LineTrait<ThreadId = ThreadIdType> + Sync + Clone + 'static,
{
    fn get(&self) -> Result<Box<dyn Workable<ThreadId = ThreadIdType>>, Error> {
        Ok(Box::new(self.line.clone()))
    }
}

impl<LineType> Add<dyn Pushable<Message = Message<LineType::Out, LineType::Signal>>>
    for GraphBuilder<LineType>
where
    LineType: LineTrait + Sync + Clone,
{
    fn add(
        &mut self,
        pushable: Box<dyn Pushable<Message = Message<LineType::Out, LineType::Signal>>>,
    ) -> Result<(), Error> {
        Add::add(&mut self.line, pushable)
    }
}

impl<LineType> Add<dyn Workable<ThreadId = <LineType as Workable>::ThreadId>>
    for GraphBuilder<LineType>
where
    LineType: LineTrait + Sync + Clone,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = <LineType as Workable>::ThreadId>>,
    ) -> Result<(), Error> {
        Add::add(&mut self.line, workable)
    }
}

pub fn make_line<In, Out, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    GraphBuilder<
        impl LineTrait<In = In, Out = Out, Signal = SignalType, LineRoutine = RoutineType>
            // Need to specify workables outside of the `LineTrait` to disambiguate.
            + Workable<ThreadId = ThreadIdType>
            + Add<dyn Workable<ThreadId = ThreadIdType>>
            + Clone
            + Send
            + Sync,
    >,
    Error,
>
where
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId + Clone,
    RoutineType: LineRoutine<In, Out> + 'static,
{
    let worker = maybe_worker?;

    Ok(GraphBuilder::new(Arc::new(Mutex::new(Line::<
        In,
        Out,
        SignalType,
        ThreadIdType,
        RoutineType,
    >::of(worker)))))
}

// Connect a synchronous worker with a non-synchronouys pullable to create synchronous
// node.
pub fn bridge_nosync<In, Out, SignalType, ThreadIdType, RoutineType, PullableType>(
    pullable: PullableType,
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    GraphBuilder<
        impl LineTrait<In = In, Out = Out, Signal = SignalType, LineRoutine = RoutineType>
            + Clone
            + Workable<ThreadId = ThreadIdType>
            + Add<dyn Workable<ThreadId = ThreadIdType>>
            + Send
            + Sync,
    >,
    Error,
>
where
    ThreadIdType: ThreadId + Clone + 'static,
    In: Clone + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + Send + Sync + 'static,
    RoutineType: 'static + LineRoutine<In, Out>,
    PullableType: Pullable<ThreadId = ThreadIdType, Message = Message<In, SignalType>> + 'static,
{
    let worker = maybe_worker?;
    let mut line = Line::of(worker);
    let bridge = Bridge::new(pullable, line.input.clone());
    line.workers.push(Box::new(bridge));

    Ok(GraphBuilder::new(Arc::new(Mutex::new(line))))
}
