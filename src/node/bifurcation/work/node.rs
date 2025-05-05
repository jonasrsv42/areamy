use crate::bifurcation;
use crate::error::Error;
use crate::node::bifurcation::routine::BifurcationRoutine;
use crate::signal::Visitors;
use crate::SyncEdge;
use crate::{DefaultThread, ThreadId};
use crate::{
    graph::{Add, Get},
    marker::Connection,
    Message, Origin, Pushable, Workable,
};
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a bifurcation.
// it has two outputs.
pub trait BifurcationTrait:
    // We can work on the line to produce output.
    Workable
    // We can add edges it should push into.
    + Add<dyn Pushable<DataType = Self::Left, SignalType = Self::Signal>, bifurcation::Left>
    + Add<dyn Pushable<DataType = Self::Right, SignalType = Self::Signal>, bifurcation::Right>

    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId>>

    // We can retrieve pushable edges
    + Get<dyn Pushable<DataType = Self::In, SignalType = Self::Signal>>
{
    // The input data entering it. 
    type In: Clone + Send + Sync + 'static;
    // The output data going out of the bifurcation.
    type Left: Clone + Send + Sync;
    type Right: Clone + Send + Sync;
    // The signal type used in the graph.
    type Signal: Origin + Clone + 'static;
    // The coroutine associated with this node.
    type BifurcationRoutine: BifurcationRoutine<Self::In, Self::Left, Self::Right>;

}

impl<BifurcationType: BifurcationTrait> BifurcationTrait for Arc<Mutex<BifurcationType>> {
    type In = BifurcationType::In;
    type Left = BifurcationType::Left;
    type Right = BifurcationType::Right;
    type Signal = BifurcationType::Signal;
    type BifurcationRoutine = BifurcationType::BifurcationRoutine;
}

pub struct Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    pub left_visitors: Visitors,
    pub right_visitors: Visitors,
    pub worker: RoutineType,

    pub workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>,

    pub left_pushes: Vec<Box<dyn Pushable<DataType = Left, SignalType = SignalType>>>,
    pub right_pushes: Vec<Box<dyn Pushable<DataType = Right, SignalType = SignalType>>>,
    pub input: Arc<SyncEdge<In, SignalType>>,
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType> Connection
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType> Workable
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn work(&mut self) -> Result<(), Error> {
        let mut push_ok;
        {
            let left_ok = self.try_left_output()?;
            let right_ok = self.try_right_output()?;

            push_ok = left_ok | right_ok;
        }
        // Otherwise we loop until we have some output.
        // To produce output we work on all the input
        // or request more input by working.
        while !push_ok {
            match self.input.poll()? {
                Some(message) => match message {
                    Message::Data(data) => {
                        self.worker.send(data)?;

                        // Try to push after performing the work, to see if we got something.
                        let left_ok = self.try_left_output()?;
                        let right_ok = self.try_right_output()?;

                        // If left or right is OK push is OK.
                        push_ok = left_ok | right_ok;
                    }
                    Message::Flush(origin) => {
                        self.worker.flush()?;

                        // Try to push after flush to see if we got something
                        self.try_left_output()?;
                        self.try_right_output()?;

                        // Forward the flush
                        push_ok = self.maybe_left_flush(&origin)?;
                        push_ok = push_ok | self.maybe_right_flush(&origin)?;
                    }
                    Message::Marker(origin) => {
                        push_ok = self.maybe_left_mark(&origin)?;
                        push_ok = push_ok | self.maybe_right_mark(&origin)?;
                    }
                },

                None => {
                    if self.workers.is_empty() {
                        self.input.wait_front()?;
                    }

                    // Then we work input from each source once.
                    for workable in self.workers.iter_mut() {
                        workable.work()?;
                    }
                }
            }
        }

        Ok(())
    }

    type ThreadId = ThreadIdType;
}

impl<In, Left, Right, SignalType, RoutineType>
    Bifurcation<In, Left, Right, SignalType, DefaultThread, RoutineType>
where
    In: Clone + Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    pub fn new(worker: RoutineType) -> Self {
        Bifurcation {
            left_visitors: Visitors::new(),
            right_visitors: Visitors::new(),
            worker,
            workers: Vec::new(),
            left_pushes: Vec::new(),
            right_pushes: Vec::new(),
            input: Arc::new(SyncEdge::new()),
        }
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    pub fn of(worker: RoutineType) -> Self {
        Bifurcation {
            left_visitors: Visitors::new(),
            right_visitors: Visitors::new(),
            worker,
            workers: Vec::new(),
            left_pushes: Vec::new(),
            right_pushes: Vec::new(),
            input: Arc::new(SyncEdge::new()),
        }
    }

    fn maybe_left_flush(&mut self, flush: &SignalType) -> Result<bool, Error> {
        if self.left_visitors.contains(flush) {
            return Ok(false);
        }

        self.left_visitors.insert(flush);
        self.push_left(Message::Flush(flush.clone()))?;

        Ok(true)
    }

    fn maybe_left_mark(&mut self, mark: &SignalType) -> Result<bool, Error> {
        if self.left_visitors.contains(mark) {
            return Ok(false);
        }

        self.left_visitors.insert(mark);
        self.push_left(Message::Marker(mark.clone()))?;

        Ok(true)
    }

    fn maybe_right_flush(&mut self, flush: &SignalType) -> Result<bool, Error> {
        // If we already visited. We do not propagate.
        if self.right_visitors.contains(flush) {
            return Ok(false);
        }

        self.right_visitors.insert(flush);
        self.push_right(Message::Flush(flush.clone()))?;

        Ok(true)
    }

    fn maybe_right_mark(&mut self, mark: &SignalType) -> Result<bool, Error> {
        // If we already visited. We do not propagate.
        if self.right_visitors.contains(mark) {
            return Ok(false);
        }

        self.right_visitors.insert(mark);
        self.push_right(Message::Marker(mark.clone()))?;

        Ok(true)
    }

    fn try_left_output(&mut self) -> Result<bool, Error> {
        // If we have output in our worker queue just immediately return it.
        match crate::Next::<Left, bifurcation::Left>::next(&mut self.worker)? {
            Some(message) => {
                self.left_visitors.clear();
                self.push_left(Message::Data(message))?;

                return Ok(true);
            }
            None => return Ok(false),
        }
    }

    fn try_right_output(&mut self) -> Result<bool, Error> {
        // If we have output in our worker queue just immediately return it.

        match crate::Next::<Right, bifurcation::Right>::next(&mut self.worker)? {
            Some(message) => {
                self.right_visitors.clear();
                self.push_right(Message::Data(message))?;

                return Ok(true);
            }
            None => return Ok(false),
        }
    }

    fn push_left(&mut self, obj: Message<Left, SignalType>) -> Result<(), Error> {
        for pushable in self.left_pushes.iter_mut() {
            pushable.push(obj.clone())?;
        }

        Ok(())
    }

    fn push_right(&mut self, obj: Message<Right, SignalType>) -> Result<(), Error> {
        for pushable in self.right_pushes.iter_mut() {
            pushable.push(obj.clone())?;
        }

        Ok(())
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType> BifurcationTrait
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    type In = In;
    type Left = Left;
    type Right = Right;
    type Signal = SignalType;
    type BifurcationRoutine = RoutineType;
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = In, SignalType = SignalType>>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn get(&self) -> Result<Box<dyn Pushable<DataType = In, SignalType = SignalType>>, Error> {
        Get::get(&self.input)
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<dyn Pushable<DataType = Left, SignalType = SignalType>, bifurcation::Left>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn add(
        &mut self,
        pushable: Box<dyn Pushable<DataType = Left, SignalType = SignalType>>,
    ) -> Result<(), Error> {
        Ok(self.left_pushes.push(pushable))
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<dyn Pushable<DataType = Right, SignalType = SignalType>, bifurcation::Right>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn add(
        &mut self,
        pushable: Box<dyn Pushable<DataType = Right, SignalType = SignalType>>,
    ) -> Result<(), Error> {
        Ok(self.right_pushes.push(pushable))
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType>>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Clone + Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn add(&mut self, workable: Box<dyn Workable<ThreadId = ThreadIdType>>) -> Result<(), Error> {
        Ok(self.workers.push(workable))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::bifurcation::routine::tests::MockBifurcation;
    use crate::{sink::work::tee, work::make_bifurcation, work::Source, Pushable};

    #[test]
    fn run_bifurcation() {
        let mut bifur = make_bifurcation(Ok(MockBifurcation::new())).unwrap();

        let mut source = Source::new(&bifur).unwrap();

        let mut left_sink = tee::Sink::new::<bifurcation::Left>(&mut bifur).unwrap();
        let mut right_sink = tee::Sink::new::<bifurcation::Right>(&mut bifur).unwrap();

        let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = bifur;

        // Add one flush
        source.push(Message::Data(1)).unwrap();
        source.push(Message::Data(2)).unwrap();

        workable.work().unwrap();
        workable.work().unwrap();

        assert_eq!(left_sink.read().unwrap(), Message::Data(2));
        assert_eq!(left_sink.read().unwrap(), Message::Data(5));

        source.push(Message::Flush("hi".into())).unwrap();
        workable.work().unwrap();

        assert_eq!(right_sink.read().unwrap(), Message::Data(3));
        assert_eq!(right_sink.read().unwrap(), Message::Data(7));

        // Now comes the flush
        match right_sink.read().unwrap() {
            Message::Flush(_) => assert!(true),
            _ => assert!(false),
        }

        match left_sink.read().unwrap() {
            Message::Flush(_) => assert!(true),
            _ => assert!(false),
        }

        source.push(Message::Data(2)).unwrap();
        workable.work().unwrap();

        assert_eq!(left_sink.read().unwrap(), Message::Data(4));
        assert_eq!(right_sink.read().unwrap(), Message::Data(6));
    }
}
