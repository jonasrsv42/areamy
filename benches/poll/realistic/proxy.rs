//! A cloud proxy, as gemproxy: one poll thread between a device and a model, full duplex.
//!
//! ```text
//! mic ─▶ into → hop×depth ─▶ model (work thread) ─▶ back → hop×(depth-1) ─▶ speaker edge
//!        └─── poll thread ──┘                       └──── poll thread ─────┘
//! ```
//!
//! The model replies to each request of `burst` frames with as many. Two remote sources wake the
//! poll thread: the bench thread (the device) and the model.
//!
//! - `burst`: the model replies in bursts while the mic pushes the next request, so both remote
//!   sources burst into the poll thread at once. An extreme reply; `depth` follows the sync
//!   stages on each leg.
//! - `steady`: the mic sends one frame per period and the model echoes each as it comes: no burst
//!   anywhere, so a change that only helps bursts shows as none here.

use crate::harness::{self, Bench, Graph, Metric, Signal};
use crate::support::{AudioFrame, SyncHop};
use areamy::error::Error;
use areamy::graph::Get;
use areamy::sync::Receiver;
use areamy::work::Line;
use areamy::{Flush, LineRoutine, Next, Pushes, Streams, ThreadBundle, ThreadId, Via, poll};
use criterion::Criterion;
use std::collections::VecDeque;
use std::mem;
use std::time::Duration;

areamy::thread_id!(ProxyThread, ModelThread);

/// Frames in an extreme reply burst, and the request before it.
const BURST: usize = 2000;
/// The steady mic's period, as the assistant's paced mic.
const MIC_PERIOD: Duration = Duration::from_micros(50);

/// Collects a request of `burst` frames, then replies with all of them at once.
struct Model {
    burst: usize,
    request: Vec<AudioFrame>,
    reply: VecDeque<AudioFrame>,
}

impl Model {
    fn new(burst: usize) -> Self {
        Model {
            burst,
            request: Vec::with_capacity(burst),
            reply: VecDeque::with_capacity(burst),
        }
    }
}

impl areamy::Send<AudioFrame> for Model {
    fn send(&mut self, frame: AudioFrame) -> Result<(), Error> {
        self.request.push(frame);
        if self.request.len() == self.burst {
            self.reply.extend(mem::take(&mut self.request));
        }
        Ok(())
    }
}

impl Next<AudioFrame> for Model {
    fn next(&mut self) -> Result<Option<AudioFrame>, Error> {
        Ok(self.reply.pop_front())
    }
}

impl Flush for Model {
    fn flush(&mut self) -> Result<(), Error> {
        self.reply.extend(self.request.drain(..));
        Ok(())
    }
}

impl LineRoutine<AudioFrame, AudioFrame> for Model {}

/// Chains one more hop onto `$node` per token: each `via` changes the type, so no loop.
macro_rules! hops {
    ($graph:ident, $node:expr; $($hop:tt)*) => {
        $node$(.via({ let _ = $hop; $graph.line(SyncHop::new) }).unwrap())*
    };
}

/// A proxy graph with `$up` hops after `into` and `$down` after `back`: `$depth` each way.
macro_rules! graph {
    ($name:ident, [$($up:tt)*], [$($down:tt)*]) => {
        fn $name<'p>(burst: usize) -> Graph<'p> {
            let mut graph = poll::Thread::<ProxyThread>::new();
            let into = graph.line(SyncHop::new).input();
            let mic: harness::Input = Get::get(&into).unwrap();
            let mut model = Line::of(Model::new(burst));
            let mut up = hops!(graph, into; $($up)*).output();
            up.pushes(&model).unwrap();
            graph.add(up);

            let back = graph.line(SyncHop::new).input();
            model.pushes(&back).unwrap();
            let mut down = hops!(graph, back; $($down)*).output();
            let speaker = Receiver::<AudioFrame, Signal>::new();
            down.pushes(&speaker).unwrap();
            graph.add(down);

            let mut threads = ThreadBundle::new();
            threads.add(graph);
            threads.add(model.stream::<ModelThread>());
            Graph::new(mic, threads, speaker)
        }
    };
}

/// Builds a proxy graph whose model replies in bursts of the given frames.
type Build = fn(usize) -> Graph<'static>;

graph!(depth_2, [1 2], [1]);
graph!(depth_8, [1 2 3 4 5 6 7 8], [1 2 3 4 5 6 7]);

pub fn proxy(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/realistic/proxy");
    let depths: [(usize, Build); 2] = [(2, depth_2), (8, depth_8)];
    for (depth, graph) in depths {
        let id = format!("burst/frames={BURST}/depth={depth}/bound=none");
        Bench::new(&mut group, &id)
            .batch(BURST)
            .run(|| graph(BURST));
        Bench::new(&mut group, &id)
            .batch(BURST)
            .metric(Metric::Cpu(ProxyThread::name()))
            .run(|| graph(BURST));
    }
    let period = MIC_PERIOD.as_micros();
    Bench::new(
        &mut group,
        &format!("steady/period={period}us/depth=2/bound=none"),
    )
    .pace(MIC_PERIOD)
    .metric(Metric::Cpu(ProxyThread::name()))
    .run(|| depth_2(1));
    group.finish();
}
