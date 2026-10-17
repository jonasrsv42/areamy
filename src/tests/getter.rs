//! Children hand out their input as whichever sink the parent names: work or poll.

use crate::edge::sync::Receiver;
use crate::error::Error;
use crate::graph::marker::Connection;
use crate::graph::{Add, Closeable, Get, Outputs, TryPush, TryPushable};
use crate::message::Message;
use crate::node::bifurcation::routine::tests::MockBifurcation;
use crate::node::bifurcation::work::node::{BifurcationTrait, Node as BifurcationNode};
use crate::node::biunion;
use crate::node::biunion::work::node::{BiunionTrait, Node as BiunionNode};
use crate::node::line::routine::tests::MockWaitLine;
use crate::node::line::work::node::{LineTrait, Node as LineNode};
use crate::node::routine::{Flush, Next, Send as RoutineSend};
use crate::work::{self, Bifurcation, Biunion, Line, Workable};
use crate::writer::pull::WriterBuffer;
use crate::{At, DefaultThread, Pushes, Trackable, closed, poll};
use alloc::collections::VecDeque;

type Signal = Trackable<&'static str>;
type PollSink<'params, D> =
    dyn poll::Sink<DataType = D, SignalType = Signal> + Send + Sync + 'params;
type WorkSink<'params, D> =
    dyn work::Sink<DataType = D, SignalType = Signal> + Send + Sync + 'params;

/// A parent naming `dyn poll::Sink`, borrowing non-`'static` state.
struct PollParent<'params, D> {
    _tag: &'params usize,
    outputs: Vec<Box<PollSink<'params, D>>>,
}

impl<D> Connection for PollParent<'_, D> {}

impl<'params, D: Send + Sync + 'static> Add<PollSink<'params, D>> for PollParent<'params, D> {
    fn add(&mut self, sink: Box<PollSink<'params, D>>) -> Result<(), Error> {
        self.outputs.push(sink);
        Ok(())
    }
}

impl<'params, D: Send + Sync + 'static> Outputs for PollParent<'params, D> {
    type Sink = PollSink<'params, D>;
}

/// Same, naming `dyn work::Sink`.
struct WorkParent<'params, D> {
    _tag: &'params usize,
    outputs: Vec<Box<WorkSink<'params, D>>>,
}

impl<D> Connection for WorkParent<'_, D> {}

impl<'params, D: Send + Sync + 'static> Add<WorkSink<'params, D>> for WorkParent<'params, D> {
    fn add(&mut self, sink: Box<WorkSink<'params, D>>) -> Result<(), Error> {
        self.outputs.push(sink);
        Ok(())
    }
}

impl<'params, D: Send + Sync + 'static> Outputs for WorkParent<'params, D> {
    type Sink = WorkSink<'params, D>;
}

/// Left and right differ in data type.
struct Mixed(VecDeque<usize>);

impl RoutineSend<usize, biunion::Left> for Mixed {
    fn send(&mut self, m: usize) -> Result<(), Error> {
        self.0.push_back(m);
        Ok(())
    }
}

impl RoutineSend<f64, biunion::Right> for Mixed {
    fn send(&mut self, m: f64) -> Result<(), Error> {
        self.0.push_back(m as usize);
        Ok(())
    }
}

impl Next<usize> for Mixed {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.0.pop_front())
    }
}

impl Flush for Mixed {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl biunion::BiunionRoutine<usize, f64, usize> for Mixed {}

type MixedBiunion = Biunion<'static, BiunionNode<usize, f64, usize, Signal, DefaultThread, Mixed>>;

#[test]
fn poll_parent_into_work_children() {
    let tag = 1;
    let mut parent = PollParent::<usize> {
        _tag: &tag,
        outputs: Vec::new(),
    };
    let receiver = Receiver::<usize, Signal>::new();
    parent.pushes(&receiver).unwrap();
    parent.pushes(&receiver.sender()).unwrap();

    let line: Line<LineNode<usize, usize, Signal, DefaultThread, MockWaitLine>> =
        Line::of(MockWaitLine::new(1));
    parent.pushes(&line).unwrap();

    let buffer = WriterBuffer::<usize, Signal, DefaultThread>::new();
    parent.pushes(&buffer).unwrap();

    let bifurcation: Bifurcation<
        BifurcationNode<usize, usize, usize, Signal, DefaultThread, MockBifurcation>,
    > = Bifurcation::of(MockBifurcation::new());
    parent.pushes(&bifurcation).unwrap();

    let biunion: MixedBiunion = Biunion::of(Mixed(VecDeque::new()));
    parent.pushes(&biunion.at_ref::<biunion::Left>()).unwrap();
    parent.pushes(&biunion.at_ref::<biunion::Left>()).unwrap();
    let mut f64_parent = PollParent::<f64> {
        _tag: &tag,
        outputs: Vec::new(),
    };
    f64_parent
        .pushes(&biunion.at_ref::<biunion::Right>())
        .unwrap();

    assert_eq!(parent.outputs.len(), 7);
    assert_eq!(f64_parent.outputs.len(), 1);
    for sink in parent.outputs.iter_mut() {
        assert_eq!(sink.try_push(Message::Data(7)).unwrap(), TryPush::Pushed);
    }
    for sink in parent.outputs.iter_mut() {
        sink.close().unwrap();
    }
    assert_eq!(receiver.read_front().unwrap(), Message::Data(7));
    assert_eq!(receiver.read_front().unwrap(), Message::Data(7));
    assert_eq!(line.input.read_front().unwrap(), Message::Data(7));
}

fn poll_parent_pushes_line<'params>(
    parent: &mut PollParent<'params, usize>,
    line: &impl LineTrait<'params, usize, usize, Signal>,
) {
    parent.pushes(line).unwrap();
}

fn poll_parent_pushes_biunion<'params>(
    left: &mut PollParent<'params, usize>,
    right: &mut PollParent<'params, f64>,
    biunion: &impl BiunionTrait<'params, usize, f64, usize, Signal>,
) {
    left.pushes(&biunion.at_ref::<biunion::Left>()).unwrap();
    right.pushes(&biunion.at_ref::<biunion::Right>()).unwrap();
}

fn poll_parent_pushes_bifurcation<'params>(
    parent: &mut PollParent<'params, usize>,
    bifurcation: &impl BifurcationTrait<'params, usize, usize, usize, Signal>,
) {
    parent.pushes(bifurcation).unwrap();
}

/// The node traits themselves promise a poll sink, not just the concrete nodes.
#[test]
fn node_traits_promise_a_poll_sink() {
    let tag = 1;
    let mut parent = PollParent::<usize> {
        _tag: &tag,
        outputs: Vec::new(),
    };
    let mut f64_parent = PollParent::<f64> {
        _tag: &tag,
        outputs: Vec::new(),
    };
    let line: Line<LineNode<usize, usize, Signal, DefaultThread, MockWaitLine>> =
        Line::of(MockWaitLine::new(1));
    poll_parent_pushes_line(&mut parent, &line);
    let biunion: Biunion<BiunionNode<usize, f64, usize, Signal, DefaultThread, Mixed>> =
        Biunion::of(Mixed(VecDeque::new()));
    poll_parent_pushes_biunion(&mut parent, &mut f64_parent, &biunion);
    let bifurcation: Bifurcation<
        BifurcationNode<usize, usize, usize, Signal, DefaultThread, MockBifurcation>,
    > = Bifurcation::of(MockBifurcation::new());
    poll_parent_pushes_bifurcation(&mut parent, &bifurcation);
    assert_eq!(parent.outputs.len(), 3);
    assert_eq!(f64_parent.outputs.len(), 1);
}

#[test]
fn work_parent_still_served() {
    let tag = 1;
    let mut parent = WorkParent::<usize> {
        _tag: &tag,
        outputs: Vec::new(),
    };
    let line: Line<LineNode<usize, usize, Signal, DefaultThread, MockWaitLine>> =
        Line::of(MockWaitLine::new(1));
    parent.pushes(&line).unwrap();
    let receiver = Receiver::<usize, Signal>::new();
    parent.pushes(&receiver).unwrap();
    let boxed: Box<dyn Get<WorkSink<'_, usize>>> = Box::new(Receiver::<usize, Signal>::new());
    parent.pushes(&boxed).unwrap();
    assert_eq!(parent.outputs.len(), 3);
}

/// A concrete `Get` beside the generic one: no overlap, both resolve.
impl Get<dyn Workable<ThreadId = DefaultThread>> for Receiver<u8, Signal> {
    fn get(&self) -> Result<Box<dyn Workable<ThreadId = DefaultThread>>, Error> {
        Err(closed!())
    }
}

#[test]
fn a_concrete_get_coexists_with_the_generic_one() {
    let receiver = Receiver::<u8, Signal>::new();
    let workable: Result<Box<dyn Workable<ThreadId = DefaultThread>>, Error> = Get::get(&receiver);
    assert!(workable.is_err());
    let mut sink: Box<WorkSink<'_, u8>> = Get::get(&receiver).unwrap();
    sink.push(Message::Data(1)).unwrap();
    assert_eq!(receiver.read_front().unwrap(), Message::Data(1));
}
