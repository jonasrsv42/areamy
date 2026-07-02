use crate::biunion;
use crate::connect::sync::Receiver;
use crate::error::{Error, ErrorKind};
use crate::node::biunion::routine::BiunionRoutine;
use crate::{
    Closeable, Message, Origin, Pushable, Sink, Workable,
    graph::{Add, Get},
};
use crate::{DefaultThread, ThreadId, marker::Connection};
use std::sync::{Arc, Mutex};

// The contract of a `Sync` node forming a biunion.
// it has two workable sources and inputs.
pub trait BiunionTrait<'params>:
    // We can work on the line to produce output.
    Workable
    // We can add edges it should push into.
    + Add<dyn Sink<DataType = Self::Out, SignalType = Self::Signal> + Send + Sync + 'params>

    // We can add things for it to work on, parents nodes.
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params, biunion::Left>
    + Add<dyn Workable<ThreadId = <Self as Workable>::ThreadId> + 'params, biunion::Right>

    // We can retrieve pushable edges
    + Get<dyn Pushable<DataType = Self::Left, SignalType = Self::Signal> + 'params, biunion::Left>
    + Get<dyn Pushable<DataType = Self::Right, SignalType = Self::Signal> + 'params, biunion::Right>

    // We can retrieve Closeable for closing the input edges.
    + Get<dyn Sink<DataType = Self::Left, SignalType = Self::Signal> + Send + Sync + 'params, biunion::Left>
    + Get<dyn Sink<DataType = Self::Right, SignalType = Self::Signal> + Send + Sync + 'params, biunion::Right>
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
impl<'params, BiunionType: BiunionTrait<'params>> BiunionTrait<'params>
    for Arc<Mutex<BiunionType>>
{
    type Left = BiunionType::Left;
    type Right = BiunionType::Right;
    type Out = BiunionType::Out;
    type Signal = BiunionType::Signal;
    type BiunionRoutine = BiunionType::BiunionRoutine;
}

/// Parent workables grouped by biunion side.
pub struct Worker<'params, ThreadIdType: ThreadId> {
    pub left: Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,
    pub right: Vec<Box<dyn Workable<ThreadId = ThreadIdType> + 'params>>,
}

impl<'params, ThreadIdType: ThreadId> Default for Worker<'params, ThreadIdType> {
    fn default() -> Self {
        Self {
            left: Vec::new(),
            right: Vec::new(),
        }
    }
}

/// Input edges grouped by biunion side.
pub struct Input<Left, Right, SignalType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    SignalType: Origin + Clone,
{
    pub left: Receiver<Left, SignalType>,
    pub right: Receiver<Right, SignalType>,
}

impl<Left, Right, SignalType> Default for Input<Left, Right, SignalType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    SignalType: Origin + Clone + Send + Sync,
{
    fn default() -> Self {
        Self {
            left: Receiver::new(),
            right: Receiver::new(),
        }
    }
}

pub struct Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    /// The coroutine of this node.
    pub routine: RoutineType,

    /// Parent workables, grouped by side.
    pub worker: Worker<'params, ThreadIdType>,

    /// Output connections. Uses Sink to support shutdown propagation.
    pub pushes: Vec<Box<dyn Sink<DataType = Out, SignalType = SignalType> + Send + Sync + 'params>>,

    /// Input edges, grouped by side.
    pub input: Input<Left, Right, SignalType>,
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> Connection
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> Workable
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn work(&mut self) -> Result<(), Error> {
        let mut push_ok = self.try_push()?;
        // Otherwise we loop until we have some output.
        // To produce output we work on all the input
        // or request more input by working.
        while !push_ok {
            let left_poll = self.input.left.poll();
            match self.propagate_if_closed(left_poll)? {
                Some(message) => push_ok = self.do_left_input(message)?,
                None => {
                    let right_poll = self.input.right.poll();
                    match self.propagate_if_closed(right_poll)? {
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
                            for i in 0..self.worker.left.len() {
                                let result = self.worker.left[i].work();
                                self.propagate_if_closed(result)?;
                            }

                            for i in 0..self.worker.right.len() {
                                let result = self.worker.right[i].work();
                                self.propagate_if_closed(result)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    type ThreadId = ThreadIdType;
}

impl<'params, Left, Right, Out, SignalType, RoutineType>
    Biunion<'params, Left, Right, Out, SignalType, DefaultThread, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    pub fn new(routine: RoutineType) -> Self {
        Biunion {
            routine,
            worker: Worker::default(),
            pushes: Vec::new(),
            input: Input::default(),
        }
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync,
    Right: Send + Sync,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    pub fn of(routine: RoutineType) -> Self {
        Biunion {
            routine,
            worker: Worker::default(),
            pushes: Vec::new(),
            input: Input::default(),
        }
    }

    fn do_left_input(&mut self, message: Message<Left, SignalType>) -> Result<bool, Error> {
        // Do work on our input or forward signals from input to output.
        match message {
            Message::Data(data) => {
                crate::Send::<Left, biunion::Left>::send(&mut self.routine, data)?;
                // If left or right is OK push is OK.
                return self.try_push();
            }
            Message::Flush(origin) => {
                self.routine.flush()?;
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
                crate::Send::<Right, biunion::Right>::send(&mut self.routine, data)?;

                // If right is OK push is OK.
                return self.try_push();
            }
            Message::Flush(origin) => {
                self.routine.flush()?;
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
        match self.routine.next()? {
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

    /// Close all push outputs. Called on shutdown to propagate close through push connections.
    fn close_pushes(&mut self) {
        for pushable in self.pushes.iter_mut() {
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

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType> BiunionTrait<'params>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    type Left = Left;
    type Right = Right;
    type Out = Out;
    type Signal = SignalType;
    type BiunionRoutine = RoutineType;
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = Left, SignalType = SignalType> + 'params, biunion::Left>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = Left, SignalType = SignalType> + 'params>, Error> {
        Get::get(&self.input.left)
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Pushable<DataType = Right, SignalType = SignalType> + 'params, biunion::Right>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<Box<dyn Pushable<DataType = Right, SignalType = SignalType> + 'params>, Error> {
        Get::get(&self.input.right)
    }
}

/// Get a [Sink] for the left input edge.
impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Sink<DataType = Left, SignalType = SignalType> + Send + Sync + 'params, biunion::Left>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = Left, SignalType = SignalType> + Send + Sync + 'params>,
        Error,
    > {
        Get::get(&self.input.left)
    }
}

/// Get a [Sink] for the right input edge.
impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Get<dyn Sink<DataType = Right, SignalType = SignalType> + Send + Sync + 'params, biunion::Right>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + Send + Sync + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn get(
        &self,
    ) -> Result<
        Box<dyn Sink<DataType = Right, SignalType = SignalType> + Send + Sync + 'params>,
        Error,
    > {
        Get::get(&self.input.right)
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType> + 'params, biunion::Left>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    ) -> Result<(), Error> {
        Ok(self.worker.left.push(workable))
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Workable<ThreadId = ThreadIdType> + 'params, biunion::Right>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn add(
        &mut self,
        workable: Box<dyn Workable<ThreadId = ThreadIdType> + 'params>,
    ) -> Result<(), Error> {
        Ok(self.worker.right.push(workable))
    }
}

impl<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
    Add<dyn Sink<DataType = Out, SignalType = SignalType> + Send + Sync + 'params>
    for Biunion<'params, Left, Right, Out, SignalType, ThreadIdType, RoutineType>
where
    Left: Send + Sync + 'static,
    Right: Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
    SignalType: Origin + Clone + 'static,
    ThreadIdType: ThreadId,
    RoutineType: BiunionRoutine<Left, Right, Out> + 'params,
{
    fn add(
        &mut self,
        closeable: Box<dyn Sink<DataType = Out, SignalType = SignalType> + Send + Sync + 'params>,
    ) -> Result<(), Error> {
        Ok(self.pushes.push(closeable))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::node::biunion::routine::tests::MockBiunion;
    use crate::{Pushable, work::Reader, work::Writer, work::make_biunion};

    #[test]
    fn run_biunion() {
        let biun = make_biunion(MockBiunion::new());

        let mut left_writer = Writer::new::<biunion::Left>(&biun).unwrap();
        let mut right_writer = Writer::new::<biunion::Right>(&biun).unwrap();

        let mut reader = Reader::new(biun).unwrap();

        left_writer.push(Message::Data(1)).unwrap();
        right_writer.push(Message::Data(2)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(2));
        assert_eq!(reader.read().unwrap(), Message::Data(7));

        left_writer.push(Message::Flush("left".into())).unwrap();
        right_writer.push(Message::Flush("right".into())).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Flush("left".into()));
        assert_eq!(reader.read().unwrap(), Message::Flush("right".into()));

        left_writer.push(Message::Data(2)).unwrap();
        right_writer.push(Message::Data(1)).unwrap();

        assert_eq!(reader.read().unwrap(), Message::Data(4));
        assert_eq!(reader.read().unwrap(), Message::Data(4));
    }

    #[test]
    fn close_propagates_through_push_when_left_input_closed() {
        let mut biun = Biunion::new(MockBiunion::new());

        let output_edge = Receiver::<usize, &'static str>::new();
        Add::<dyn Sink<DataType = usize, SignalType = &'static str> + Send + Sync>::add(
            &mut biun,
            Box::new(output_edge.sender()),
        )
        .unwrap();

        biun.input.left.close().unwrap();

        let result = biun.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            output_edge.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn close_propagates_through_push_when_right_input_closed() {
        let mut biun = Biunion::new(MockBiunion::new());

        let output_edge = Receiver::<usize, &'static str>::new();
        Add::<dyn Sink<DataType = usize, SignalType = &'static str> + Send + Sync>::add(
            &mut biun,
            Box::new(output_edge.sender()),
        )
        .unwrap();

        biun.input.right.close().unwrap();

        let result = biun.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            output_edge.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }

    #[test]
    fn close_propagates_through_work_chain() {
        use crate::closed;
        use crate::marker::Connection;

        // Create a mock workable that returns Closed error
        struct ClosingWorkable;
        impl Connection for ClosingWorkable {}
        impl Workable for ClosingWorkable {
            type ThreadId = DefaultThread;
            fn work(&mut self) -> Result<(), Error> {
                Err(closed!())
            }
        }

        let mut biun = Biunion::new(MockBiunion::new());

        // Add a workable that returns Closed
        Add::<dyn Workable<ThreadId = DefaultThread>, biunion::Left>::add(
            &mut biun,
            Box::new(ClosingWorkable),
        )
        .unwrap();

        let output_edge = Receiver::<usize, &'static str>::new();
        Add::<dyn Sink<DataType = usize, SignalType = &'static str> + Send + Sync>::add(
            &mut biun,
            Box::new(output_edge.sender()),
        )
        .unwrap();

        let result = biun.work();
        assert!(matches!(result.unwrap_err().kind, ErrorKind::Closed));

        assert!(matches!(
            output_edge.poll().unwrap_err().kind,
            ErrorKind::Closed
        ));
    }
}
