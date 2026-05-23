use crate::bifurcation;
use crate::connect::sync::Receiver;
use crate::error::{Error, ErrorKind};
use crate::node::bifurcation::routine::BifurcationRoutine;
use crate::{
    Closeable, Message, Origin, Pushable, Workable,
    graph::{Add, Get},
    marker::Connection,
};
use crate::{DefaultThread, ThreadId};
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a bifurcation.
// it has two outputs.
pub trait BifurcationTrait:
    // We can work on the line to produce output.
    Workable
    // We can add edges it should push into.
    + Add<dyn Closeable<DataType = Self::Left, SignalType = Self::Signal> + Send + Sync, bifurcation::Left>
    + Add<dyn Closeable<DataType = Self::Right, SignalType = Self::Signal> + Send + Sync, bifurcation::Right>

    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId>>

    // We can retrieve pushable edges
    + Get<dyn Pushable<DataType = Self::In, SignalType = Self::Signal>>

    // We can retrieve a Closeable for closing the input edge.
    + Get<dyn Closeable<DataType = Self::In, SignalType = Self::Signal> + Send + Sync>
{
    // The input data entering it. 
    type In: Send + Sync + 'static;
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

/// Output push connections grouped by bifurcation side.
pub struct Pushes<Left, Right, SignalType>
where
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    pub left: Vec<Box<dyn Closeable<DataType = Left, SignalType = SignalType> + Send + Sync>>,
    pub right: Vec<Box<dyn Closeable<DataType = Right, SignalType = SignalType> + Send + Sync>>,
}

impl<Left, Right, SignalType> Default for Pushes<Left, Right, SignalType>
where
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    fn default() -> Self {
        Self {
            left: Vec::new(),
            right: Vec::new(),
        }
    }
}

pub struct Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    /// The coroutine of this node.
    pub routine: RoutineType,

    /// Parent workables.
    pub workers: Vec<Box<dyn Workable<ThreadId = ThreadIdType>>>,

    /// Output connections, grouped by side.
    pub pushes: Pushes<Left, Right, SignalType>,

    /// Input edge.
    pub input: Receiver<In, SignalType>,
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType> Connection
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
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
    In: Send + Sync,
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
            let poll_result = self.input.poll();
            match self.propagate_if_closed(poll_result)? {
                Some(message) => match message {
                    Message::Data(data) => {
                        self.routine.send(data)?;

                        // Try to push after performing the work, to see if we got something.
                        push_ok = self.try_output()?
                    }
                    Message::Flush(origin) => {
                        self.routine.flush()?;

                        // Try to push after flush to see if we got something
                        self.try_output()?;

                        // Forward the flush
                        self.push_left(Message::Flush(origin.clone()))?;
                        self.push_right(Message::Flush(origin))?;

                        push_ok = true;
                    }
                    Message::Marker(origin) => {
                        self.push_left(Message::Marker(origin.clone()))?;
                        self.push_right(Message::Marker(origin))?;
                        push_ok = true;
                    }
                },

                None => {
                    if self.workers.is_empty() {
                        let wait_result = self.input.wait_front();
                        self.propagate_if_closed(wait_result)?;
                    }

                    // Then we work input from each source once.
                    for i in 0..self.workers.len() {
                        let result = self.workers[i].work();
                        self.propagate_if_closed(result)?;
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
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    pub fn new(routine: RoutineType) -> Self {
        Bifurcation {
            routine,
            workers: Vec::new(),
            pushes: Pushes::default(),
            input: Receiver::new(),
        }
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    pub fn of(routine: RoutineType) -> Self {
        Bifurcation {
            routine,
            workers: Vec::new(),
            pushes: Pushes::default(),
            input: Receiver::new(),
        }
    }

    fn try_output(&mut self) -> Result<bool, Error> {
        // Try to push after performing the work, to see if we got something.
        let left_ok = self.try_left_output()?;
        let right_ok = self.try_right_output()?;

        // If left or right is OK push is OK.
        Ok(left_ok | right_ok)
    }

    fn try_left_output(&mut self) -> Result<bool, Error> {
        // If we have output in our worker queue just immediately return it.
        match crate::Next::<Left, bifurcation::Left>::next(&mut self.routine)? {
            Some(message) => {
                self.push_left(Message::Data(message))?;

                return Ok(true);
            }
            None => return Ok(false),
        }
    }

    fn try_right_output(&mut self) -> Result<bool, Error> {
        // If we have output in our worker queue just immediately return it.

        match crate::Next::<Right, bifurcation::Right>::next(&mut self.routine)? {
            Some(message) => {
                self.push_right(Message::Data(message))?;

                return Ok(true);
            }
            None => return Ok(false),
        }
    }

    fn push_left(&mut self, obj: Message<Left, SignalType>) -> Result<(), Error> {
        for pushable in self.pushes.left.iter_mut() {
            pushable.push(obj.clone())?;
        }

        Ok(())
    }

    fn push_right(&mut self, obj: Message<Right, SignalType>) -> Result<(), Error> {
        for pushable in self.pushes.right.iter_mut() {
            pushable.push(obj.clone())?;
        }

        Ok(())
    }

    /// Close all push outputs. Called on shutdown to propagate close through push connections.
    fn close_pushes(&mut self) {
        for pushable in self.pushes.left.iter_mut() {
            let _ = pushable.close();
        }
        for pushable in self.pushes.right.iter_mut() {
            let _ = pushable.close();
        }
    }

    /// If result is a Closed error, close all pushes to propagate shutdown.
    fn propagate_if_closed<T>(&mut self, result: Result<T, Error>) -> Result<T, Error> {
        result.map_err(|e| {
            if matches!(e.kind, ErrorKind::Closed) {
                self.close_pushes();
            }
            e
        })
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType> BifurcationTrait
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
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
    In: Send + Sync + 'static,
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

/// Get a [Closeable] for this node's input edge.
impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Get<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Closeable<DataType = In, SignalType = SignalType> + Send + Sync>, Error>
    {
        Get::get(&self.input)
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<dyn Closeable<DataType = Left, SignalType = SignalType> + Send + Sync, bifurcation::Left>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn add(
        &mut self,
        closeable: Box<dyn Closeable<DataType = Left, SignalType = SignalType> + Send + Sync>,
    ) -> Result<(), Error> {
        Ok(self.pushes.left.push(closeable))
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<dyn Closeable<DataType = Right, SignalType = SignalType> + Send + Sync, bifurcation::Right>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
    Left: Clone + Send + Sync,
    Right: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BifurcationRoutine<In, Left, Right>,
{
    fn add(
        &mut self,
        closeable: Box<dyn Closeable<DataType = Right, SignalType = SignalType> + Send + Sync>,
    ) -> Result<(), Error> {
        Ok(self.pushes.right.push(closeable))
    }
}

impl<In, Left, Right, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType>>
    for Bifurcation<In, Left, Right, SignalType, ThreadIdType, RoutineType>
where
    In: Send + Sync + 'static,
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
    use crate::closed;
    use crate::node::bifurcation::routine::tests::MockBifurcation;
    use crate::{Pushable, sink::work::tee, work::Source, work::make_bifurcation};

    #[test]
    fn run_bifurcation() {
        let mut bifur = make_bifurcation(MockBifurcation::new());

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

    #[test]
    fn close_propagates_through_push_when_input_closed() {
        let mut bifur = Bifurcation::new(MockBifurcation::new());

        let left_output = Receiver::<usize, &'static str>::new();
        let right_output = Receiver::<usize, &'static str>::new();

        Add::<
            dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync,
            bifurcation::Left,
        >::add(&mut bifur, Box::new(left_output.sender()))
        .unwrap();
        Add::<
            dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync,
            bifurcation::Right,
        >::add(&mut bifur, Box::new(right_output.sender()))
        .unwrap();

        bifur.input.close().unwrap();

        let result = bifur.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            left_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
        assert!(matches!(
            right_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn close_propagates_through_work_chain() {
        // Create a mock workable that returns Closed error
        struct ClosingWorkable;
        impl Connection for ClosingWorkable {}
        impl Workable for ClosingWorkable {
            type ThreadId = DefaultThread;
            fn work(&mut self) -> Result<(), Error> {
                Err(closed!())
            }
        }

        let mut bifur = Bifurcation::new(MockBifurcation::new());

        // Add a workable that returns Closed
        Add::<dyn Workable<ThreadId = DefaultThread>>::add(&mut bifur, Box::new(ClosingWorkable))
            .unwrap();

        let left_output = Receiver::<usize, &'static str>::new();
        let right_output = Receiver::<usize, &'static str>::new();

        Add::<
            dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync,
            bifurcation::Left,
        >::add(&mut bifur, Box::new(left_output.sender()))
        .unwrap();
        Add::<
            dyn Closeable<DataType = usize, SignalType = &'static str> + Send + Sync,
            bifurcation::Right,
        >::add(&mut bifur, Box::new(right_output.sender()))
        .unwrap();

        let result = bifur.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            left_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
        assert!(matches!(
            right_output.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }
}
