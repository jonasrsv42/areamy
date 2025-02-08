use crate::error::Error;
use crate::node::line::sync::bridge::Bridge;
use crate::node::line::sync::node::LineTrait;
use crate::{
    sync::Line, AddPushable, AddWorkable, GetPushable, GetWorkable, LineRoutine, Message, Origin,
    Pullable, Pushable, ThreadId, Workable,
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

impl<LineType> GetWorkable for GraphBuilder<LineType>
where
    LineType: LineTrait + GetPushable + Sync + Clone + 'static,
{
    type Workable = LineType;
    fn get(&self) -> Result<Self::Workable, Error> {
        Ok(self.line.clone())
    }
}

impl<LineType> GetPushable for GraphBuilder<LineType>
where
    LineType: LineTrait + GetPushable + Sync + Clone + 'static,
{
    type Pushable = LineType::Pushable;

    fn get(&self) -> Result<Self::Pushable, Error> {
        GetPushable::get(&self.line)
    }
}

impl<LineType> AddPushable for GraphBuilder<LineType>
where
    LineType:
        LineTrait + AddPushable<Message = Message<LineType::Out, LineType::Signal>> + Sync + Clone,
{
    type Message = Message<LineType::Out, LineType::Signal>;
    fn add<PushableType: Pushable<Message = Message<LineType::Out, LineType::Signal>> + 'static>(
        &mut self,
        pushable: PushableType,
    ) -> Result<(), Error> {
        AddPushable::add(&mut self.line, pushable)
    }
}

impl<LineType> AddWorkable for GraphBuilder<LineType>
where
    LineType: LineTrait + AddWorkable<ThreadId = <LineType as Workable>::ThreadId> + Sync + Clone,
{
    type ThreadId = <LineType as Workable>::ThreadId;
    fn add<WorkableType: Workable<ThreadId = Self::ThreadId> + 'static>(
        &mut self,
        workable: WorkableType,
    ) -> Result<(), Error> {
        AddWorkable::add(&mut self.line, workable)
    }
}

pub fn make_line<In, Out, SignalType, ThreadIdType, RoutineType>(
    maybe_worker: Result<RoutineType, Error>,
) -> Result<
    GraphBuilder<
        impl LineTrait<In = In, Out = Out, Signal = SignalType, LineRoutine = RoutineType>
            // Need to specify workables outside of the `LineTrait` to disambiguate.
            + Workable<ThreadId = ThreadIdType>
            + AddWorkable<ThreadId = ThreadIdType>
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
            + AddWorkable<ThreadId = ThreadIdType>
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
