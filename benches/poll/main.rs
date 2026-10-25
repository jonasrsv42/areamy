//! Poll runtime benches. Filter by group: `cargo bench --bench poll -- realistic`.
//!
//! - `poll/micro/*`: one routine between two sync edges.
//! - `poll/realistic/*`: graphs shaped like downstream's, and the harness's own floor.
//! - `poll/scale/*`: graphs of up to 10240 ids, for how the scheduler scales with graph size.

#[cfg(feature = "trace")]
compile_error!("benches time with `trace` off: its logs fire on every full edge");

mod harness;
mod micro;
mod realistic;
mod scale;
mod support;

use criterion::{criterion_group, criterion_main};

criterion_group!(
    benches,
    micro::future_line,
    realistic::assistant::assistant,
    realistic::floor::floor,
    realistic::proxy::proxy,
    scale::sparse,
    scale::burst,
    scale::remote
);
criterion_main!(benches);
