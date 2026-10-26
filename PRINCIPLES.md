# Principles

Areamy trades flexibility for compile-time guarantees. A wrong graph shouldn't compile.

## Ownership is the scheduling graph

- `via` / `schedules` / `reader` / `Pullable::then` **move** the upstream into its owner. Each node has one owner: a downstream, or its thread if it's a root.
- Ownership is a forest rooted at threads, so scheduling cycles can't be written.
- `pushes` / `tee` / `.input()` **borrow**: the upstream holds a `Sender` to the downstream's input. Push edges may form cycles; they carry data, never scheduling. Back-edges use `edge::policy::FollowData`.
- Fan-out: one consumer owns the node, the rest get `pushes` or `tee`.

This rules out designs that decouple nodes from ownership: arenas, index or handle graphs, and runtime-validated topologies. They turn a type error into a runtime check.

## Thread identity is a type

- Every node carries a `ThreadId`, and connections require matching ids.
- Blueprints are `Send`. Thread-affine state (`Rc` edges, local wakers, `!Send` routines) is created on the thread it lives on.
- Work and pull nodes are standalone; their `ThreadId` is inferred from wiring.
- Poll nodes are minted by their thread (`&mut self`) because they reserve their nodes eagerly. A minted node that is never added fails at build.

## Explicit over implicit

- The connection kind is chosen at the call site. Ownership, scheduling and edge cost are visible where they happen.
- Routine traits are implemented explicitly: each impl whitelists a valid In→Out pair. No blanket impls.
- Data-type annotations (`via::<T>`, `pushes::<T>`) are welcome; they document the graph.
- Sides are named, never inferred from data types: `at` moves a side (owning edges), `at_mut` / `at_ref` borrow it.

## Pay at build time, not per message

- Allocation, boxing and binding happen while the graph is built. Nodes and upstreams are passed by value; a `Box` appears only where a node stores an upstream as `dyn`.
- The hot path avoids extra heap allocation, locks and syscalls where placement allows.

## Close travels like data

- A node owns sender handles, never an edge. Closing a sender ends that producer only; an edge closes once every producer into it is done. Closing a receiver closes the edge for all.
- A merge ends with its last input: fan-in, writers into one node, and both biunion sides alike. There are no half-closed graphs to support, only no data lost to which source closed first.
- Flush, then close, delivers what each source flushed, along owning paths (a `pushes` side path collapses back-to-back signals). Close is fast: anything unflushed may drop. Pinned in `src/tests/close`.
- A source that never closes (a long-lived config writer) keeps its merge, and everything downstream, alive: close every source, or stop the bundle.
- Errors are not closes: the first error tears down its thread, and `ThreadBundle` stops the rest.

## Layers

- `graph` is the vocabulary (`Outlet`, `TryPushable`, `Pushable`, `Closeable`, `Schedule`, `Add`, `Get`, `Outputs`, `graph::Sink`) and declares the wiring verbs (`Via` / `Downstream`, `Pushes`, `At`); the runtimes and `edge` implement them. Outside its tests and doc links it uses only `message`, `wake`, `signal` and `error`.
- `message` and `wake` are the currencies every layer carries: what flows, and how a node is woken. `wake` uses nothing else in the crate.
- `work` and `poll` are runtimes built on `graph`, each with its own capabilities (`work::Sink`; `poll::Context`, `poll::Sink`). Runtime types stay in their runtime: `poll::Context` carries the scheduler's timer beside the `wake` wakers.
- `edge` holds the concrete edges between nodes and runtimes (`sync::Sender`, `policy::FollowData`, `Fanout`, the `Pushes` impl), so it uses `work` and `poll` types. The runtimes use `edge` back (their `FollowsData` impls, `work` on `edge::sync`): these are modules of one crate, not a strict stack. The rule is placement: a capability is implemented next to its type, and runtime types stay in their runtime.

## Small surface

- Prefer an existing idiom at the call site over new API.
- Remove what is unused or wrong, and migrate downstream. Don't deprecate.
- `ErrorKind::Closed` is control flow: `?` propagates shutdown.
