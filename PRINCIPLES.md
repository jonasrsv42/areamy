# Principles

Areamy trades flexibility for compile-time guarantees. A wrong graph shouldn't compile.

## Ownership is the scheduling graph

- `work::Bidi` / `work::Schedule` / `Pullable::then` / poll `.parent()` **move** the parent into the child. Each node has one owner: a child, or its thread if it's a root.
- Ownership is a forest rooted at threads, so scheduling cycles can't be written.
- `Push` / `.input::<Sync>()` **borrow**: the parent holds a `Sender` to the child's input. Push edges may form cycles; they carry data, never scheduling. Back-edges use `SignalPolicy::FollowData`.
- Fan-out: one consumer owns the node, the rest get `push`.

This rules out designs that decouple nodes from ownership: arenas, index or handle graphs, and runtime-validated topologies. They turn a type error into a runtime check.

## Thread identity is a type

- Every node carries a `ThreadId`, and connections require matching ids.
- Blueprints are `Send`. Thread-affine state (`Rc` edges, local wakers, `!Send` routines) is created on the thread it lives on.
- Work and pull nodes are standalone; their `ThreadId` is inferred from wiring.
- Poll nodes are minted by their thread (`&mut self`) because they reserve wake slots eagerly. A minted node that is never added fails at build.

## Explicit over implicit

- The connection kind is chosen at the call site. Ownership, scheduling and edge cost are visible where they happen.
- Routine traits are implemented explicitly: each impl whitelists a valid In→Out pair. No blanket impls.
- Data-type annotations (`Push::<T>`, `work::Bidi::<T>`) are welcome; they document the graph. Sides are named on the node (`node.at::<Left>()`).

## Pay at build time, not per message

- Allocation, boxing and binding happen while the graph is built. Nodes and parents are passed by value; a `Box` appears only where a node stores a parent as `dyn`.
- The hot path avoids extra heap allocation, locks and syscalls where placement allows.

## Layers

- `graph` is the vocabulary (`Outlet`, `TryPushable`, `Pushable`, `Closeable`, `Add`, `Get`, `Outputs`, `graph::Sink`). Outside its tests and doc links it uses only `message`, `signal` and `error`.
- `work` and `poll` are runtimes built on it, each with its own capabilities (`work::Sink`; `poll::Room`, `poll::Waker`, `poll::Sink`). Runtime types stay in their runtime: `poll::Waker` carries the scheduler's timers.
- `edge` holds the concrete edges between nodes and runtimes (`sync::Sender`, `PolicyEdge`, `Fanout`, `Push`), so it uses `work` and `poll` types. The runtimes use `edge` back (their `Policied` impls, `work` on `edge::sync`): these are modules of one crate, not a strict stack. The rule is placement: a capability is implemented next to its type, and runtime types stay in their runtime.

## Small surface

- Prefer an existing idiom at the call site over new API.
- Remove what is unused or wrong, and migrate downstream. Don't deprecate.
- `ErrorKind::Closed` is control flow: `?` propagates shutdown.
