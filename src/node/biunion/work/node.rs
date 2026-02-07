use crate::SyncEdge;
use crate::biunion;
use crate::error::Error;
use crate::node::biunion::routine::BiunionRoutine;
use crate::{DefaultThread, ThreadId, marker::Connection};
use crate::{
    Message, Origin, Pushable, Workable,
    graph::{Add, Get},
};
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a biunion.
// it has two workable sources and inputs.
pub trait BiunionTrait:
    // We can work on the line to produce output.
    Workable
    // We can add edges it should push into.
    + Add<dyn Pushable<DataType = Self::Out, SignalType = Self::Signal>>

    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId>, biunion::Left>
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId>, biunion::Right>

    // We can retrieve pushable edges
    + Get<dyn Pushable<DataType = Self::Left, SignalType = Self::Signal>, biunion::Left>
    + Get<dyn Pushable<DataType = Self::Right, SignalType = Self::Signal>, biunion::Right>

    // We can retrieve Sources for closing the input edges.
    + Get<dyn crate::GraphPushSource<DataType = Self::Left, SignalType = Self::Signal>, biunion::Left>
    + Get<dyn crate::GraphPushSource<DataType = Self::Right, SignalType = Self::Signal>, biunion::Right>
{
    // The input data going into the line.
    type Left:  Send + Sync + 'static;
    type Right:  Send + Sync + 'static;
    // The output data leaving it..
    type Out: Clone + Send + Sync;
    // The signal type used in the graph.
    type Signal: Origin + Clone + 'static;
    // The coroutine associated with this node.
    type BiunionRoutine: BiunionRoutine<Self::Left, Self::Right, Self::Out>;

}

// A `Send+Sync` variant of our node. Looks how neatly all the
// `AddWorkable` and `GetPushable` parameters are automatically derived
// for this since those traits are generic over our builders :))
impl<BiunionType: BiunionTrait> BiunionTrait for Arc<Mutex<BiunionType>> {
    type Left = BiunionType::Left;
    type Right = BiunionType::Right;
    type Out = BiunionType::Out;
    type Signal = BiunionType::Signal;
    type BiunionRoutine = BiunionType::BiunionRoutine;
}

pub struct Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    // The coroutine of this node.
    pub worker: RoutineType,

    // Parent workers.
    pub left_workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>,
    pub right_workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>,

    // Output connection.
    pub pushes: Vec<Box<dyn Pushable<DataType = Out, SignalType = SignalType>>>,

    // Input queues.
    pub left_input: Arc<SyncEdge<Left, SignalType>>,
    pub right_input: Arc<SyncEdge<Right, SignalType>>,
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType> Connection
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType> Workable
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn work(&mut self) -> Result<(), Error> {
        let mut push_ok = self.try_push()?;
        // Otherwise we loop until we have some output.
        // To produce output we work on all the input
        // or request more input by working.
        while !push_ok {
            match self.left_input.poll()? {
                Some(message) => push_ok = self.do_left_input(message)?,
                None => match self.right_input.poll()? {
                    Some(message) => push_ok = self.do_right_input(message)?,
                    None => {
                        // We always work left and right. This is problematic if
                        // one workable is much more active than the other. If there is a use-case for this
                        // we may need to make workables limited blocking.. or adaptive.. or throw more threads
                        // at it.
                        //
                        // However the primary use-case for biunion now involves connecting one
                        // input only as pushable to send reset signals. So won't fix now.
                        //
                        // Future me may complain.
                        //
                        // If Workables are emtpy we also do spinlocking for now. Need to think about that.
                        for workable in self.left_workers.iter_mut() {
                            workable.work()?;
                        }

                        for workable in self.right_workers.iter_mut() {
                            workable.work()?;
                        }
                    }
                },
            }
        }

        Ok(())
    }

    type ThreadId = ThreadIdType;
}

impl<Left, Right, Out, SignalType, RoutineType>
    Biunion<Left, Right, Out, SignalType, DefaultThread, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    pub fn new(worker: RoutineType) -> Self {
        Biunion {
            worker,
            left_workers: Vec::new(),
            right_workers: Vec::new(),
            pushes: Vec::new(),
            left_input: Arc::new(SyncEdge::new()),
            right_input: Arc::new(SyncEdge::new()),
        }
    }
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    pub fn of(worker: RoutineType) -> Self {
        Biunion {
            worker,
            left_workers: Vec::new(),
            right_workers: Vec::new(),
            pushes: Vec::new(),
            left_input: Arc::new(SyncEdge::new()),
            right_input: Arc::new(SyncEdge::new()),
        }
    }

    fn do_left_input(&mut self, message: Message<Left, SignalType>) -> Result<bool, Error> {
        // Do work on our input or forward signals from input to output.
        match message {
            Message::Data(data) => {
                crate::Send::<Left, biunion::Left>::send(&mut self.worker, data)?;
                // If left or right is OK push is OK.
                return self.try_push();
            }
            Message::Flush(origin) => {
                self.worker.flush()?;
                // If left or right is OK push is OK.
                self.try_push()?;

                self.push(Message::Flush(origin.clone()))?;
                return Ok(true);
            }
            Message::Marker(origin) => {
                self.push(Message::Marker(origin.clone()))?;
                return Ok(true);
            }
        }
    }

    fn do_right_input(&mut self, message: Message<Right, SignalType>) -> Result<bool, Error> {
        // Do work on our input or forward signals from input to output.
        match message {
            Message::Data(data) => {
                crate::Send::<Right, biunion::Right>::send(&mut self.worker, data)?;

                // If right is OK push is OK.
                return self.try_push();
            }
            Message::Flush(origin) => {
                self.worker.flush()?;
                // If left or right is OK push is OK.
                self.try_push()?;

                self.push(Message::Flush(origin.clone()))?;
                return Ok(true);
            }
            Message::Marker(origin) => {
                self.push(Message::Marker(origin.clone()))?;

                return Ok(true);
            }
        }
    }

    fn try_push(&mut self) -> Result<bool, Error> {
        // If we have output in our worker queue just immediately return it.
        match self.worker.next()? {
            Some(message) => {
                self.push(Message::Data(message))?;

                return Ok(true);
            }
            None => Ok(false),
        }
    }

    fn push(&mut self, obj: Message<Out, SignalType>) -> Result<(), Error> {
        for pushable in self.pushes.iter_mut() {
            pushable.push(obj.clone())?;
        }

        Ok(())
    }
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType> BiunionTrait
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    type Left = Left;
    type Right = Right;
    type Out = Out;
    type Signal = SignalType;
    type BiunionRoutine = RoutineType;
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = Left, SignalType = SignalType>, biunion::Left>
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = Left, SignalType = SignalType>>, Error> {
        Get::get(&self.left_input)
    }
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = Right, SignalType = SignalType>, biunion::Right>
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = Right, SignalType = SignalType>>, Error> {
        Get::get(&self.right_input)
    }
}

/// Get a [crate::GraphPushSource] for the left input edge.
impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn crate::GraphPushSource<DataType = Left, SignalType = SignalType>, biunion::Left>
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn get(
        &self,
    ) -> Result<Box<dyn crate::GraphPushSource<DataType = Left, SignalType = SignalType>>, Error>
    {
        Get::get(&self.left_input)
    }
}

/// Get a [crate::GraphPushSource] for the right input edge.
impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn crate::GraphPushSource<DataType = Right, SignalType = SignalType>, biunion::Right>
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn get(
        &self,
    ) -> Result<Box<dyn crate::GraphPushSource<DataType = Right, SignalType = SignalType>>, Error>
    {
        Get::get(&self.right_input)
    }
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType>, biunion::Left>
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn add(&mut self, workable: Box<dyn Workable<ThreadId = ThreadIdType>>) -> Result<(), Error> {
        Ok(self.left_workers.push(workable))
    }
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType>, biunion::Right>
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn add(&mut self, workable: Box<dyn Workable<ThreadId = ThreadIdType>>) -> Result<(), Error> {
        Ok(self.right_workers.push(workable))
    }
}

impl<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Pushable<DataType = Out, SignalType = SignalType>>
    for Biunion<Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out>,
{
    fn add(
        &mut self,
        pushable: Box<dyn Pushable<DataType = Out, SignalType = SignalType>>,
    ) -> Result<(), Error> {
        Ok(self.pushes.push(pushable))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::biunion::routine::tests::MockBiunion;
    use crate::{Pushable, work::Sink, work::Source, work::make_biunion};

    #[test]
    fn run_biunion() {
        let biun = make_biunion(MockBiunion::new());

        let mut left_source = Source::new::<biunion::Left>(&biun).unwrap();
        let mut right_source = Source::new::<biunion::Right>(&biun).unwrap();

        let mut sink = Sink::new(biun).unwrap();

        left_source.push(Message::Data(1)).unwrap();
        right_source.push(Message::Data(2)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(2));
        assert_eq!(sink.read().unwrap(), Message::Data(7));

        left_source.push(Message::Flush("left".into())).unwrap();
        right_source.push(Message::Flush("right".into())).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Flush("left".into()));
        assert_eq!(sink.read().unwrap(), Message::Flush("right".into()));

        left_source.push(Message::Data(2)).unwrap();
        right_source.push(Message::Data(1)).unwrap();

        assert_eq!(sink.read().unwrap(), Message::Data(4));
        assert_eq!(sink.read().unwrap(), Message::Data(4));
    }
}
