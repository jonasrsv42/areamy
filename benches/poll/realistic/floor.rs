//! The harness with no graph: the input is the output edge's own sender. What a realistic
//! bench costs before areamy's runtime does anything; a gain that brings a bench near this
//! is hidden by it.

use crate::harness::{Bench, Graph, Output};
use areamy::ThreadBundle;
use criterion::Criterion;

fn graph<'p>() -> Graph<'p> {
    let output = Output::new();
    Graph::new(Box::new(output.sender()), ThreadBundle::new(), output)
}

pub fn floor(c: &mut Criterion) {
    let mut group = c.benchmark_group("poll/realistic");
    Bench::new(&mut group, "floor").run(graph);
    group.finish();
}
