# Benches

One criterion target per area. Benchmark ids are `area/scenario/params`, parameters as
`key=value` and last, so a regex picks a subset:

| Target | Ids | What |
|---|---|---|
| `edge` | `edge/{same_thread,cross_thread}/*` | the raw sync edge every work connection uses |
| `work` | `work/*` | work lines, fan-out, biunion, helper threads |
| `pull` | `pull/*` | pull lines |
| `poll` | `poll/micro/*` | one routine between two sync edges |
| | `poll/realistic/*` | graphs shaped like downstream's (`assistant`: firmware, `proxy`: gemproxy), and the harness's floor |
| | `poll/scale/*` | graphs of 64 to 10240 ids, mostly idle (`sparse`, `remote`) or all fed at once (`burst`): how the scheduler scales with graph size |

```bash
cargo bench --bench poll -- realistic     # one area, one group
cargo bench -- 'bound=8/'                 # a parameter across areas; the `/` keeps out bound=80
```

`benches/common.rs` holds routines every target shares; `benches/<area>/` holds an area's own,
such as `poll/harness.rs`, which drives the realistic graphs.

## Metrics

Realistic and scale ids end in their metric:

- `wall`: the pipeline's throughput, wakes and waits included.
- `cpu=<thread>`: on-CPU time of that thread, kernel time included (Linux, 64-bit), for when
  another thread sets the wall time, such as a slow speaker. Criterion's throughput line then
  means frames per CPU-second of that thread.

Each realistic graph has bursty ids and a steady one (`paced`, `steady`): judge a change on both,
so one that helps bursts doesn't cost the steady stream.

`poll/realistic/floor` is the realistic harness with no graph. A bench near it is measuring the
harness, not areamy.

## Comparing a change

Name baselines explicitly. Criterion keys saved results by id only, not by build, so an
implicit comparison can span different features or machines.

```bash
cargo bench --bench poll -- realistic --save-baseline before   # without the change
cargo bench --bench poll -- realistic --baseline before        # with it
```

Criterion's verdicts assume a quiet machine, so it reports drift as significant change. Run
before and after alternately, at least three times each, and compare against the id's spread
between runs of identical code (five runs each, 2026-10-19):

| Id | Pinned (`2-7`) | Unpinned |
|---|---|---|
| `poll/realistic/assistant/unpaced/bound=8/*` | ~3% | ~3% |
| `poll/realistic/assistant/unpaced/bound=none/*` | ~3% | ~1% |
| `poll/realistic/assistant/slow_speaker/*` | <1% | ~1% |
| `poll/realistic/floor/*` | ~10% | ~10% |

Not yet measured this way: `assistant/paced` and `proxy/*`. Over three alternating rounds they
spread ~1–3% (2026-10-21), `proxy/burst` up to ~5% and `assistant/paced` up to ~6%
(2026-10-25).

`unpaced/bound=8` also shifts level with core placement: about 15% faster when its threads land
on SMT siblings, which hand off wakes sooner. Compare its baselines only from the same `taskset`
set in one session. The floor settles at one of two levels per run; read it as rough.

The output thread's frame count is checked at the end of a run, after criterion has saved its
samples: discard the baseline of a run that fails there.

Benches refuse to build with `trace`: its logs fire on every full edge.

## Noise

Pin and fix the clock for any comparison, so placement and frequency stay the same between runs:

```bash
sudo cpupower frequency-set -g performance   # powersave scales the clock mid-run
taskset -c 2-7 cargo bench ...               # off busy cores
```

Leave at least one core per bench thread (the realistic benches use three), or their hand-offs
take turns on a core instead of running side by side. `lscpu -e` shows SMT siblings: where cpu
`n` and `n+8` share a core, `2-7` puts every thread on its own core.

## Adding a bench

- Put it under its area's target, or add a target (`[[bench]]` in `Cargo.toml`) for a new area,
  with the `trace` guard at its root.
- Put parameters in the id (`bound=8/batch=1000`). Adding a point is fine, but changing what an
  existing id measures, a constant outside the id included, breaks every saved baseline: give
  it a new id. Burst ids queue every iteration of a sample at once, so `--measurement-time` is
  such a constant for them.
- Keep allocation out of the timed loop: circulate payloads, or return outputs from
  `iter_batched` so criterion drops them after the clock stops.
- Check correctness inside the timed closure, after the clock stops: a check after
  `bench_function` runs once criterion has already saved the result.
